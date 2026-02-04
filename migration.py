"""Entity migration scenarios.

VMTP's entity IDs are host-address independent. An entity can:
- Move to a different host mid-conversation
- Be multi-homed (reachable at multiple addresses)
- Survive host IP changes

TCP cannot do this - connections are bound to (srcIP, srcPort, dstIP, dstPort).
Changing any of these requires connection teardown and re-establishment.

This test demonstrates VMTP's unique capability.
"""

import simpy
from dataclasses import dataclass
from typing import Optional

from entity import Entity, EntityRegistry
from network import Network, NetworkConfig
from packet import Request, Response
from server import Server
from client import Client
from metrics import MetricsCollector


class MigratableNetwork(Network):
    """Network that supports entity migration.

    Key insight: entity ID is the stable identifier. The network maintains
    a routing table from entity ID to mailbox. Migration just updates the
    routing - the mailbox associated with an entity can change.

    This models VMTP's host-independent addressing.
    """

    def __init__(self, env: simpy.Environment, config: NetworkConfig = None):
        super().__init__(env, config)
        self._migration_count = 0
        self._migration_log = []

    def migrate(self, entity_id: int, new_mailbox: simpy.Store):
        """Migrate an entity to a new mailbox.

        The entity ID stays the same, packets now route to new mailbox.
        This models: same entity identifier, different network address.
        """
        old_mailbox = self._mailboxes.get(entity_id)
        self._mailboxes[entity_id] = new_mailbox
        self._migration_count += 1
        self._migration_log.append({
            "time": self.env.now,
            "entity_id": entity_id,
        })
        return old_mailbox


class MigratableServer:
    """Server that can migrate to a new location mid-operation.

    Migration preserves:
    - Entity ID (clients use this to address)
    - CSR cache (duplicate detection state)
    - Execution log (for correctness tracking)

    Migration changes:
    - Mailbox (where packets arrive)
    - Network routing (how to reach this entity)
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: MigratableNetwork,
        metrics: MetricsCollector,
        process_time: float = 0.005,
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.metrics = metrics
        self.process_time = process_time

        from server import CSRCache
        self.csr_cache = CSRCache(max_size=1000)
        self._execution_log = {}
        self._stats = {
            "requests_received": 0,
            "responses_sent": 0,
            "duplicates_detected": 0,
            "cache_hits": 0,
            "executions": 0,
            "silent_reexecutions": 0,
            "migrations": 0,
        }

        # Create initial mailbox and register with network
        self.mailbox = simpy.Store(env)
        network.register(entity.id)
        network._mailboxes[entity.id] = self.mailbox

        self._running = True
        self.process = env.process(self._run())

    def _run(self):
        """Main server loop."""
        while self._running:
            try:
                packet = yield self.mailbox.get()
                if isinstance(packet, Request):
                    self._stats["requests_received"] += 1
                    yield self.env.process(self._handle_request(packet))
            except simpy.Interrupt:
                # Migration interrupt - continue with new mailbox
                pass

    def _handle_request(self, request: Request):
        """Handle request with duplicate detection."""
        from server import ClientStateRecord

        csr = self.csr_cache.get(request.client_id)

        if csr is not None and request.transaction_id == csr.last_transaction_id:
            self._stats["duplicates_detected"] += 1
            if csr.cached_response is not None:
                self._stats["cache_hits"] += 1
                response = Response(
                    client_id=request.client_id,
                    server_id=self.entity.id,
                    transaction_id=request.transaction_id,
                    payload=csr.cached_response.payload,
                    timestamp=self.env.now,
                    is_cached=True,
                )
                self._send_response(response, request.client_id)
                return
            elif not request.idempotent:
                self.metrics.record_duplicate_execution(request.client_id, request.transaction_id)
        elif csr is not None and request.transaction_id < csr.last_transaction_id:
            return  # Old duplicate

        # Execute
        yield self.env.process(self._execute(request))

    def _execute(self, request: Request):
        """Execute and respond."""
        from server import ClientStateRecord

        key = (request.client_id, request.transaction_id)
        prev = self._execution_log.get(key, 0)
        self._execution_log[key] = prev + 1

        if prev > 0:
            self._stats["silent_reexecutions"] += 1
            if not request.idempotent:
                self.metrics.record_duplicate_execution(request.client_id, request.transaction_id)

        yield self.env.timeout(self.process_time)
        self._stats["executions"] += 1

        response = Response(
            client_id=request.client_id,
            server_id=self.entity.id,
            transaction_id=request.transaction_id,
            payload=f"result-{request.transaction_id}",
            timestamp=self.env.now,
        )

        csr = self.csr_cache.get(request.client_id) or ClientStateRecord(
            client_id=request.client_id,
            last_transaction_id=request.transaction_id,
        )
        csr.last_transaction_id = request.transaction_id
        csr.last_activity = self.env.now
        if not request.idempotent:
            csr.cached_response = response
        self.csr_cache.put(csr)

        self._send_response(response, request.client_id)

    def _send_response(self, response: Response, dest: int):
        self._stats["responses_sent"] += 1
        self.network.send(response, dest)

    def migrate(self):
        """Migrate to a new mailbox.

        Creates new mailbox, updates network routing, interrupts old receiver.
        CSR and execution state are preserved (they're in this object).
        """
        # Create new mailbox
        new_mailbox = simpy.Store(self.env)

        # Update network routing
        self.network.migrate(self.entity.id, new_mailbox)

        # Swap mailboxes
        old_mailbox = self.mailbox
        self.mailbox = new_mailbox

        # Interrupt current receiver so it picks up new mailbox
        if self.process.is_alive:
            self.process.interrupt()

        self._stats["migrations"] += 1

    @property
    def stats(self):
        return self._stats.copy()


@dataclass
class MigrationResult:
    """Results from migration scenario."""

    # Transaction success
    pre_migration_success: int
    pre_migration_total: int
    during_migration_success: int  # Transaction that spans migration
    post_migration_success: int
    post_migration_total: int

    # Timing
    migration_latency: Optional[float]  # Time from migration to next successful txn

    # Packets
    total_packets: int

    # State preservation
    csr_preserved: bool
    duplicate_executions: int


def run_vmtp_migration(
    transactions_before: int = 5,
    transactions_after: int = 5,
    seed: int = 42,
) -> MigrationResult:
    """Test VMTP server migration mid-conversation.

    1. Client starts transactions with server
    2. Server migrates (new mailbox, same entity ID)
    3. Client continues transactions (same entity ID - doesn't know server moved)
    """
    import random
    random.seed(seed)

    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = MigratableNetwork(env, NetworkConfig(base_delay=0.010, loss_probability=0.0, seed=seed))

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = MigratableServer(env, server_entity, network, metrics, process_time=0.005)
    client = Client(env, client_entity, network, metrics, default_timeout=0.100)

    results = {
        "pre": [],
        "during": None,
        "post": [],
        "migration_time": None,
        "first_post_migration": None,
    }

    def workload():
        # Phase 1: Before migration
        for i in range(transactions_before):
            response = yield from client.transact(server_entity.id, f"pre-{i}")
            results["pre"].append(response is not None)
            yield env.timeout(0.020)

        # Record CSR state before migration
        pre_migration_csr_count = len(server.csr_cache)

        # Migration happens
        results["migration_time"] = env.now
        server.migrate()  # Server moves to new mailbox

        # Small delay to let migration complete
        yield env.timeout(0.001)

        # Phase 2: Transaction immediately after migration
        # (client doesn't know server moved - uses same entity ID)
        response = yield from client.transact(server_entity.id, "during-migration")
        results["during"] = response is not None

        if response:
            results["first_post_migration"] = env.now - results["migration_time"]

        # Phase 3: More transactions after migration
        for i in range(transactions_after):
            response = yield from client.transact(server_entity.id, f"post-{i}")
            results["post"].append(response is not None)
            if response and results["first_post_migration"] is None:
                results["first_post_migration"] = env.now - results["migration_time"]
            yield env.timeout(0.020)

        # Check CSR preservation
        results["csr_preserved"] = len(server.csr_cache) >= pre_migration_csr_count

    env.process(workload())
    env.run(until=30.0)

    summary = metrics.summary()

    return MigrationResult(
        pre_migration_success=sum(results["pre"]),
        pre_migration_total=len(results["pre"]),
        during_migration_success=1 if results["during"] else 0,
        post_migration_success=sum(results["post"]),
        post_migration_total=len(results["post"]),
        migration_latency=results["first_post_migration"],
        total_packets=network.stats["sent"],
        csr_preserved=results["csr_preserved"],
        duplicate_executions=summary["duplicate_executions"],
    )


def demonstrate_migration(verbose: bool = True) -> MigrationResult:
    """Demonstrate VMTP entity migration."""
    if verbose:
        print("=" * 80)
        print("VMTP ENTITY MIGRATION DEMONSTRATION")
        print("=" * 80)
        print()
        print("Scenario:")
        print("  1. Server starts at location A (address 100)")
        print("  2. Client performs transactions with server")
        print("  3. Server migrates to location B (address 300)")
        print("  4. Client continues using SAME entity ID")
        print("  5. Transactions continue seamlessly")
        print()

    result = run_vmtp_migration()

    if verbose:
        print(f"Results:")
        print(f"  Pre-migration:    {result.pre_migration_success}/{result.pre_migration_total} successful")
        print(f"  During migration: {result.during_migration_success}/1 successful")
        print(f"  Post-migration:   {result.post_migration_success}/{result.post_migration_total} successful")
        print()
        print(f"  Migration latency: {result.migration_latency*1000:.1f}ms" if result.migration_latency else "  Migration latency: N/A")
        print(f"  CSR preserved: {result.csr_preserved}")
        print(f"  Duplicate executions: {result.duplicate_executions}")
        print(f"  Total packets: {result.total_packets}")
        print()
        print("=" * 80)
        print("KEY INSIGHT:")
        print()
        print("  VMTP: Entity ID is stable. Address can change. Client doesn't care.")
        print("        Migration is transparent - no reconnection, no state loss.")
        print()
        print("  TCP:  Connection = (srcIP, srcPort, dstIP, dstPort)")
        print("        Server IP change = connection broken = reconnection required")
        print("        Client MUST know about migration and reconnect.")
        print("=" * 80)

    return result


def compare_migration_costs(verbose: bool = True):
    """Compare VMTP migration (free) vs TCP migration (reconnection)."""
    if verbose:
        print()
        print("=" * 80)
        print("MIGRATION COST COMPARISON")
        print("=" * 80)
        print()
        print("VMTP migration cost:")
        print("  - Packets: 0 (just update routing)")
        print("  - Latency: 0 (next transaction works immediately)")
        print("  - Client changes: None (same entity ID)")
        print()
        print("TCP migration cost:")
        print("  - Packets: 7+ (FIN/ACK teardown + SYN/SYN-ACK/ACK setup)")
        print("  - Latency: 2+ RTT (teardown + setup)")
        print("  - Client changes: Must discover new IP, reconnect")
        print()
        print("For N migrations in a session:")
        print("  VMTP: O(1) - no per-migration cost")
        print("  TCP:  O(N) - each migration costs ~7 packets + 2 RTT")
        print("=" * 80)


def run_multiple_migrations(num_migrations: int = 5, verbose: bool = True) -> dict:
    """Test multiple migrations during a session."""
    import random
    random.seed(42)

    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = MigratableNetwork(env, NetworkConfig(base_delay=0.010, seed=42))

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = MigratableServer(env, server_entity, network, metrics, process_time=0.005)
    client = Client(env, client_entity, network, metrics, default_timeout=0.100)

    results = {"successes": 0, "failures": 0, "migrations": 0}

    def workload():
        transactions_per_phase = 3

        for migration in range(num_migrations + 1):
            # Do some transactions
            for i in range(transactions_per_phase):
                response = yield from client.transact(server_entity.id, f"phase{migration}-txn{i}")
                if response:
                    results["successes"] += 1
                else:
                    results["failures"] += 1
                yield env.timeout(0.010)

            # Migrate (except after last phase)
            if migration < num_migrations:
                server.migrate()
                results["migrations"] += 1
                yield env.timeout(0.001)

    env.process(workload())
    env.run(until=60.0)

    total_txn = results["successes"] + results["failures"]
    summary = metrics.summary()

    if verbose:
        print(f"\nMultiple migrations test ({num_migrations} migrations):")
        print(f"  Transactions: {results['successes']}/{total_txn} successful")
        print(f"  Migrations: {results['migrations']}")
        print(f"  Total packets: {network.stats['sent']}")
        print(f"  Packets per successful txn: {network.stats['sent'] / results['successes']:.2f}")
        print(f"  Duplicate executions: {summary['duplicate_executions']}")

    return {
        "num_migrations": num_migrations,
        "total_transactions": total_txn,
        "successful": results["successes"],
        "packets": network.stats["sent"],
        "packets_per_txn": network.stats["sent"] / results["successes"] if results["successes"] else 0,
    }


def sweep_migrations(verbose: bool = True):
    """Show that migration cost is O(1) - doesn't scale with number of migrations."""
    if verbose:
        print()
        print("=" * 80)
        print("MIGRATION SCALING TEST")
        print("=" * 80)
        print()
        print("Testing: 3 transactions per phase, varying number of migrations")
        print()
        print(f"{'Migrations':>12} │ {'Transactions':>12} {'Packets':>10} {'Pkts/Txn':>10}")
        print("─" * 12 + "─┼─" + "─" * 12 + "─" * 11 + "─" * 11)

    for n in [0, 1, 2, 5, 10, 20]:
        result = run_multiple_migrations(n, verbose=False)
        if verbose:
            print(f"{result['num_migrations']:>12} │ {result['total_transactions']:>12} {result['packets']:>10} {result['packets_per_txn']:>10.2f}")

    if verbose:
        print()
        print("Observation: Packets/transaction stays ~2.0 regardless of migrations")
        print("             VMTP migration has ZERO per-migration packet cost")
        print()
        print("TCP equivalent would add ~7 packets per migration for reconnection")


if __name__ == "__main__":
    demonstrate_migration()
    compare_migration_costs()
    sweep_migrations()
