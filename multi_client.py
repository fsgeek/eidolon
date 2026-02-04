"""Multi-client scenarios to test CSR eviction and concurrent transactions.

Key VMTP claim: servers CAN discard Client State Records at any time
without affecting correctness. Let's stress-test that.
"""

import simpy
import random
from dataclasses import dataclass

from entity import EntityRegistry
from network import Network, NetworkConfig
from server import Server
from client import Client
from metrics import MetricsCollector


@dataclass
class MultiClientResult:
    """Results from multi-client scenario."""

    num_clients: int
    transactions_per_client: int
    csr_capacity: int
    loss_rate: float

    total_transactions: int
    successful: int
    failed: int
    duplicate_executions: int

    csr_evictions: int  # How many times CSR was at capacity
    server_executions: int
    server_duplicates_detected: int

    @property
    def success_rate(self) -> float:
        return self.successful / self.total_transactions if self.total_transactions else 0

    @property
    def correctness_maintained(self) -> bool:
        return self.duplicate_executions == 0


def run_multi_client(
    num_clients: int,
    transactions_per_client: int,
    csr_capacity: int,
    loss_rate: float = 0.1,
    seed: int = 42,
    idempotent_ratio: float = 0.5,
) -> MultiClientResult:
    """Run scenario with multiple concurrent clients.

    If num_clients > csr_capacity, the server MUST evict CSRs,
    testing whether correctness is maintained.
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=loss_rate, seed=seed),
    )

    # Create server with limited CSR capacity
    server_entity = registry.create(name="server")
    server = Server(
        env,
        server_entity,
        network,
        metrics,
        process_time=0.005,
        csr_capacity=csr_capacity,
    )

    # Create multiple clients
    clients = []
    for i in range(num_clients):
        client_entity = registry.create(name=f"client-{i}")
        client = Client(env, client_entity, network, metrics, default_timeout=0.050)
        clients.append((client_entity, client))

    # Track CSR pressure
    csr_high_water = [0]

    def client_workload(client_entity, client, client_idx):
        """Workload for a single client."""
        # Stagger start times slightly
        yield env.timeout(client_idx * 0.002)

        for i in range(transactions_per_client):
            idempotent = random.random() < idempotent_ratio
            yield from client.transact(
                server_id=server_entity.id,
                payload=f"client-{client_idx}-op-{i}",
                idempotent=idempotent,
            )
            # Variable delay between transactions
            yield env.timeout(random.uniform(0.001, 0.010))

            # Track CSR count
            csr_high_water[0] = max(csr_high_water[0], len(server.csr_cache))

    # Start all client workloads
    for idx, (client_entity, client) in enumerate(clients):
        env.process(client_workload(client_entity, client, idx))

    # Run simulation
    env.run(until=600.0)  # 10 minute max

    summary = metrics.summary()

    return MultiClientResult(
        num_clients=num_clients,
        transactions_per_client=transactions_per_client,
        csr_capacity=csr_capacity,
        loss_rate=loss_rate,
        total_transactions=summary["total_transactions"],
        successful=summary["successful"],
        failed=summary["timed_out"],
        duplicate_executions=summary["duplicate_executions"],
        csr_evictions=max(0, csr_high_water[0] - csr_capacity),
        server_executions=server.stats["executions"],
        server_duplicates_detected=server.stats["duplicates_detected"],
    )


def run_csr_pressure_test(verbose: bool = True) -> list[MultiClientResult]:
    """Test CSR eviction under increasing client pressure."""
    if verbose:
        print("=" * 80)
        print("CSR PRESSURE TEST: Server capacity vs client population")
        print("=" * 80)
        print()
        print("Testing: 10 transactions per client, 10% packet loss")
        print("CSR capacity fixed at 5 (forcing eviction when clients > 5)")
        print()
        print(f"{'Clients':>8} │ {'Success':>8} {'DupExec':>8} {'Evictions':>10} │ {'Status':>12}")
        print("─" * 8 + "─┼─" + "─" * 8 + "─" * 9 + "─" * 11 + "─┼─" + "─" * 12)

    results = []
    csr_capacity = 5

    for num_clients in [2, 5, 10, 20, 50]:
        result = run_multi_client(
            num_clients=num_clients,
            transactions_per_client=10,
            csr_capacity=csr_capacity,
            loss_rate=0.10,
            seed=42,
            idempotent_ratio=0.5,
        )
        results.append(result)

        if verbose:
            status = "✓ CORRECT" if result.correctness_maintained else "✗ BUG!"
            evictions = "yes" if num_clients > csr_capacity else "no"
            print(
                f"{num_clients:>8} │ "
                f"{result.success_rate*100:>7.1f}% "
                f"{result.duplicate_executions:>8} "
                f"{evictions:>10} │ "
                f"{status:>12}"
            )

    if verbose:
        total_dups = sum(r.duplicate_executions for r in results)
        print()
        if total_dups == 0:
            print("✓ No duplicate executions detected")
        else:
            print(f"⚠ {total_dups} duplicate executions detected!")
            print()
            print("Key insight: CSR eviction is ONLY safe for idempotent operations.")
            print("When CSR is evicted and a non-idempotent retransmit arrives,")
            print("the server re-executes without knowing it's a duplicate.")
            print()
            print("The RFC addresses this: server should PROBE the client to find")
            print("the last completed transaction before re-executing non-idempotent ops.")
            print("This minimal implementation doesn't implement probing.")

    return results


def run_concurrent_stress(verbose: bool = True) -> MultiClientResult:
    """Maximum stress: many clients, small CSR, high loss."""
    if verbose:
        print("=" * 80)
        print("STRESS TEST: 100 clients, CSR capacity 10, 20% loss")
        print("=" * 80)
        print()

    result = run_multi_client(
        num_clients=100,
        transactions_per_client=5,
        csr_capacity=10,
        loss_rate=0.20,
        seed=42,
        idempotent_ratio=0.5,
    )

    if verbose:
        print(f"Total transactions: {result.total_transactions}")
        print(f"Successful: {result.successful} ({result.success_rate*100:.1f}%)")
        print(f"Failed (timeout): {result.failed}")
        print(f"Duplicate executions: {result.duplicate_executions}")
        print()
        print(f"Server executions: {result.server_executions}")
        print(f"Server duplicates detected: {result.server_duplicates_detected}")
        print()
        if result.correctness_maintained:
            print("✓ CORRECTNESS MAINTAINED under extreme CSR pressure")
        else:
            print("✗ CORRECTNESS VIOLATION - duplicate executions detected!")

    return result


if __name__ == "__main__":
    random.seed(42)
    run_csr_pressure_test()
    print()
    run_concurrent_stress()
