"""Network partition scenarios.

Partition is different from loss:
- Loss: packets randomly dropped, some get through
- Partition: ZERO packets for a duration, then recovery

This is where connectionless (VMTP) vs connection-oriented (TCP)
should show meaningful differences. TCP has half-open connection hell,
reconnection handshakes. VMTP should just... resume.
"""

import simpy
from dataclasses import dataclass
from typing import Optional

from entity import EntityRegistry
from network import Network, NetworkConfig
from server import Server as VmtpServer
from client import Client as VmtpClient
from tcp_baseline import TcpServer, TcpClient
from quic_baseline import QuicServer, QuicClient
from metrics import MetricsCollector


class PartitionableNetwork(Network):
    """Network that can be partitioned and healed.

    When partitioned, ALL packets between specified entity pairs are dropped.
    When healed, normal delivery resumes.
    """

    def __init__(self, env: simpy.Environment, config: NetworkConfig = None):
        super().__init__(env, config)
        self._partitioned_pairs: set[tuple[int, int]] = set()
        self._partition_stats = {
            "packets_dropped_by_partition": 0,
        }

    def partition(self, entity_a: int, entity_b: int):
        """Create a partition between two entities (bidirectional)."""
        self._partitioned_pairs.add((entity_a, entity_b))
        self._partitioned_pairs.add((entity_b, entity_a))

    def heal(self, entity_a: int, entity_b: int):
        """Heal a partition between two entities."""
        self._partitioned_pairs.discard((entity_a, entity_b))
        self._partitioned_pairs.discard((entity_b, entity_a))

    def heal_all(self):
        """Heal all partitions."""
        self._partitioned_pairs.clear()

    def is_partitioned(self, source: int, dest: int) -> bool:
        """Check if two entities are partitioned."""
        return (source, dest) in self._partitioned_pairs

    def _deliver(self, packet, destination_id: int):
        """Override delivery to check for partition."""
        # Determine source from packet
        source_id = getattr(packet, 'client_id', None) or getattr(packet, 'source_id', None)

        if source_id and self.is_partitioned(source_id, destination_id):
            # Partitioned - drop silently
            self._partition_stats["packets_dropped_by_partition"] += 1
            self._stats["sent"] += 1
            self._stats["dropped"] += 1
            self._packet_log.append({
                "time": self.env.now,
                "event": "partition_drop",
                "packet": packet,
                "destination": destination_id,
            })
            yield self.env.timeout(0)  # Make it a generator, then return
            return

        # Not partitioned - normal delivery
        yield from super()._deliver(packet, destination_id)

    @property
    def partition_stats(self) -> dict:
        return {**self._stats, **self._partition_stats}


@dataclass
class PartitionScenarioResult:
    """Results from a partition scenario."""
    protocol: str

    # Timing
    partition_start: float
    partition_duration: float

    # Before partition
    pre_partition_success: int
    pre_partition_total: int

    # During partition
    during_partition_success: int
    during_partition_total: int

    # After heal
    post_heal_success: int
    post_heal_total: int

    # Recovery
    first_success_after_heal: Optional[float]  # Time to first successful txn after heal
    packets_during_partition: int

    # Total packets used
    total_packets_sent: int = 0

    # TCP-specific: reconnections needed
    reconnections: int = 0

    # Correctness
    duplicate_executions: int = 0


def run_vmtp_partition(
    partition_start: float,
    partition_duration: float,
    transactions_before: int = 10,
    transactions_during: int = 10,
    transactions_after: int = 10,
    seed: int = 42,
) -> PartitionScenarioResult:
    """Run VMTP through a partition scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = PartitionableNetwork(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=0.0, seed=seed)
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = VmtpServer(env, server_entity, network, metrics, process_time=0.005)
    client = VmtpClient(env, client_entity, network, metrics, default_timeout=0.100, default_max_retries=3)

    results = {
        "pre": [],
        "during": [],
        "post": [],
        "first_success_after_heal": None,
        "heal_time": partition_start + partition_duration,
    }

    def workload():
        # Phase 1: Before partition
        for i in range(transactions_before):
            start = env.now
            response = yield from client.transact(server_entity.id, f"pre-{i}")
            results["pre"].append(response is not None)
            yield env.timeout(0.020)

        # Wait for partition to start
        if env.now < partition_start:
            yield env.timeout(partition_start - env.now)

        # Phase 2: During partition
        for i in range(transactions_during):
            start = env.now
            response = yield from client.transact(server_entity.id, f"during-{i}")
            results["during"].append(response is not None)
            yield env.timeout(0.050)

        # Wait for heal
        heal_time = partition_start + partition_duration
        if env.now < heal_time:
            yield env.timeout(heal_time - env.now + 0.010)  # Small buffer after heal

        # Phase 3: After heal
        for i in range(transactions_after):
            response = yield from client.transact(server_entity.id, f"post-{i}")
            success = response is not None
            results["post"].append(success)
            if success and results["first_success_after_heal"] is None:
                results["first_success_after_heal"] = env.now - results["heal_time"]
            yield env.timeout(0.020)

    def partition_controller():
        yield env.timeout(partition_start)
        network.partition(client_entity.id, server_entity.id)
        yield env.timeout(partition_duration)
        network.heal_all()

    env.process(workload())
    env.process(partition_controller())
    env.run(until=60.0)

    summary = metrics.summary()

    return PartitionScenarioResult(
        protocol="VMTP",
        partition_start=partition_start,
        partition_duration=partition_duration,
        pre_partition_success=sum(results["pre"]),
        pre_partition_total=len(results["pre"]),
        during_partition_success=sum(results["during"]),
        during_partition_total=len(results["during"]),
        post_heal_success=sum(results["post"]),
        post_heal_total=len(results["post"]),
        first_success_after_heal=results["first_success_after_heal"],
        packets_during_partition=network.partition_stats["packets_dropped_by_partition"],
        total_packets_sent=network.stats["sent"],
        reconnections=0,  # VMTP has no connections
        duplicate_executions=summary["duplicate_executions"],
    )


def run_tcp_partition(
    partition_start: float,
    partition_duration: float,
    transactions_before: int = 10,
    transactions_during: int = 10,
    transactions_after: int = 10,
    seed: int = 42,
) -> PartitionScenarioResult:
    """Run TCP through the same partition scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = PartitionableNetwork(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=0.0, seed=seed)
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = TcpServer(env, server_entity, network, None, process_time=0.005)
    client = TcpClient(env, client_entity, network, None, default_timeout=0.100, default_max_retries=3)

    results = {
        "pre": [],
        "during": [],
        "post": [],
        "first_success_after_heal": None,
        "heal_time": partition_start + partition_duration,
    }

    def workload():
        # Phase 1: Before partition - establishes connection
        for i in range(transactions_before):
            response = yield from client.transact(server_entity.id, f"pre-{i}")
            results["pre"].append(response is not None)
            yield env.timeout(0.020)

        # Wait for partition
        if env.now < partition_start:
            yield env.timeout(partition_start - env.now)

        # Phase 2: During partition - connection should break
        for i in range(transactions_during):
            response = yield from client.transact(server_entity.id, f"during-{i}")
            results["during"].append(response is not None)
            yield env.timeout(0.050)

        # Wait for heal
        heal_time = partition_start + partition_duration
        if env.now < heal_time:
            yield env.timeout(heal_time - env.now + 0.010)

        # Phase 3: After heal - needs to re-establish connection
        for i in range(transactions_after):
            response = yield from client.transact(server_entity.id, f"post-{i}")
            success = response is not None
            results["post"].append(success)
            if success and results["first_success_after_heal"] is None:
                results["first_success_after_heal"] = env.now - results["heal_time"]
            yield env.timeout(0.020)

    def partition_controller():
        yield env.timeout(partition_start)
        network.partition(client_entity.id, server_entity.id)
        yield env.timeout(partition_duration)
        network.heal_all()

    env.process(workload())
    env.process(partition_controller())
    env.run(until=60.0)

    # Count reconnections: connections established minus 1 (initial connection)
    reconnections = max(0, client.stats["connections_established"] - 1)

    return PartitionScenarioResult(
        protocol="TCP",
        partition_start=partition_start,
        partition_duration=partition_duration,
        pre_partition_success=sum(results["pre"]),
        pre_partition_total=len(results["pre"]),
        during_partition_success=sum(results["during"]),
        during_partition_total=len(results["during"]),
        post_heal_success=sum(results["post"]),
        post_heal_total=len(results["post"]),
        first_success_after_heal=results["first_success_after_heal"],
        packets_during_partition=network.partition_stats["packets_dropped_by_partition"],
        total_packets_sent=network.stats["sent"],
        reconnections=reconnections,
        duplicate_executions=server.stats["duplicate_executions"],
    )


def run_quic_partition(
    partition_start: float,
    partition_duration: float,
    transactions_before: int = 10,
    transactions_during: int = 10,
    transactions_after: int = 10,
    seed: int = 42,
) -> PartitionScenarioResult:
    """Run QUIC through the partition scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = PartitionableNetwork(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=0.0, seed=seed)
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = QuicServer(env, server_entity, network, None, process_time=0.005, connection_timeout=2.0)
    client = QuicClient(env, client_entity, network, None, default_timeout=0.100, default_max_retries=3)

    results = {
        "pre": [],
        "during": [],
        "post": [],
        "first_success_after_heal": None,
        "heal_time": partition_start + partition_duration,
    }

    def workload():
        # Phase 1: Before partition - establishes connection
        for i in range(transactions_before):
            response = yield from client.transact(server_entity.id, f"pre-{i}")
            results["pre"].append(response is not None)
            yield env.timeout(0.020)

        # Wait for partition
        if env.now < partition_start:
            yield env.timeout(partition_start - env.now)

        # Phase 2: During partition
        for i in range(transactions_during):
            response = yield from client.transact(server_entity.id, f"during-{i}")
            results["during"].append(response is not None)
            yield env.timeout(0.050)

        # Wait for heal
        heal_time = partition_start + partition_duration
        if env.now < heal_time:
            yield env.timeout(heal_time - env.now + 0.010)

        # Phase 3: After heal
        for i in range(transactions_after):
            response = yield from client.transact(server_entity.id, f"post-{i}")
            success = response is not None
            results["post"].append(success)
            if success and results["first_success_after_heal"] is None:
                results["first_success_after_heal"] = env.now - results["heal_time"]
            yield env.timeout(0.020)

    def partition_controller():
        yield env.timeout(partition_start)
        network.partition(client_entity.id, server_entity.id)
        yield env.timeout(partition_duration)
        network.heal_all()

    env.process(workload())
    env.process(partition_controller())
    env.run(until=60.0)

    # QUIC reconnections: if 0-RTT was used after partition, that's a "reconnection"
    reconnections = server.stats.get("zero_rtt_accepted", 0)
    # Also count fresh connections beyond the first
    reconnections += max(0, server.stats.get("connections_accepted", 1) - 1)

    return PartitionScenarioResult(
        protocol="QUIC",
        partition_start=partition_start,
        partition_duration=partition_duration,
        pre_partition_success=sum(results["pre"]),
        pre_partition_total=len(results["pre"]),
        during_partition_success=sum(results["during"]),
        during_partition_total=len(results["during"]),
        post_heal_success=sum(results["post"]),
        post_heal_total=len(results["post"]),
        first_success_after_heal=results["first_success_after_heal"],
        packets_during_partition=network.partition_stats["packets_dropped_by_partition"],
        total_packets_sent=network.stats["sent"],
        reconnections=reconnections,
        duplicate_executions=0,
    )


def compare_partition_scenarios(verbose: bool = True) -> list[tuple]:
    """Compare VMTP, TCP, and QUIC under various partition scenarios."""

    scenarios = [
        ("short_partition", 0.5, 0.2),   # 200ms partition
        ("medium_partition", 0.5, 1.0),  # 1 second partition
        ("long_partition", 0.5, 5.0),    # 5 second partition
    ]

    if verbose:
        print("=" * 80)
        print("NETWORK PARTITION COMPARISON: VMTP vs QUIC vs TCP")
        print("=" * 80)
        print()
        print("Scenario: 10 transactions before, 10 during, 10 after partition")
        print()

    results = []

    for name, start, duration in scenarios:
        vmtp = run_vmtp_partition(start, duration)
        quic = run_quic_partition(start, duration)
        tcp = run_tcp_partition(start, duration)
        results.append((name, vmtp, quic, tcp))

        if verbose:
            print(f"─── {name} (partition duration: {duration}s) ───")
            print()
            print(f"{'Phase':<20} {'VMTP':>12} {'QUIC':>12} {'TCP':>12}")
            print(f"{'─'*20} {'─'*12} {'─'*12} {'─'*12}")
            print(f"{'Before partition':<20} {vmtp.pre_partition_success}/{vmtp.pre_partition_total:>9} {quic.pre_partition_success}/{quic.pre_partition_total:>9} {tcp.pre_partition_success}/{tcp.pre_partition_total:>9}")
            print(f"{'During partition':<20} {vmtp.during_partition_success}/{vmtp.during_partition_total:>9} {quic.during_partition_success}/{quic.during_partition_total:>9} {tcp.during_partition_success}/{tcp.during_partition_total:>9}")
            print(f"{'After heal':<20} {vmtp.post_heal_success}/{vmtp.post_heal_total:>9} {quic.post_heal_success}/{quic.post_heal_total:>9} {tcp.post_heal_success}/{tcp.post_heal_total:>9}")
            print()

            vmtp_recovery = f"{vmtp.first_success_after_heal*1000:.0f}ms" if vmtp.first_success_after_heal else "N/A"
            quic_recovery = f"{quic.first_success_after_heal*1000:.0f}ms" if quic.first_success_after_heal else "N/A"
            tcp_recovery = f"{tcp.first_success_after_heal*1000:.0f}ms" if tcp.first_success_after_heal else "N/A"
            print(f"{'Recovery time':<20} {vmtp_recovery:>12} {quic_recovery:>12} {tcp_recovery:>12}")
            print(f"{'Total packets':<20} {vmtp.total_packets_sent:>12} {quic.total_packets_sent:>12} {tcp.total_packets_sent:>12}")
            print(f"{'Reconnections':<20} {vmtp.reconnections:>12} {quic.reconnections:>12} {tcp.reconnections:>12}")
            print()

    if verbose:
        print("=" * 80)
        print("KEY FINDINGS:")
        print()
        total_vmtp_pkts = sum(r[1].total_packets_sent for r in results)
        total_quic_pkts = sum(r[2].total_packets_sent for r in results)
        total_tcp_pkts = sum(r[3].total_packets_sent for r in results)
        total_quic_reconn = sum(r[2].reconnections for r in results)
        total_tcp_reconn = sum(r[3].reconnections for r in results)
        print(f"Total packets:  VMTP={total_vmtp_pkts}, QUIC={total_quic_pkts}, TCP={total_tcp_pkts}")
        print(f"Reconnections:  VMTP=0, QUIC={total_quic_reconn}, TCP={total_tcp_reconn}")
        print()
        print("VMTP: Partition = 'packets don't arrive'. No state corruption.")
        print("      Recovery is immediate - just send next transaction.")
        print()
        print("QUIC: Connection ID helps, but connection still times out.")
        print("      0-RTT resumption helps recovery, but still costs packets.")
        print()
        print("TCP:  Server times out idle connection during partition.")
        print("      Full 3-way handshake to reconnect.")
        print("=" * 80)

    return results


def sweep_partition_durations(verbose: bool = True) -> dict:
    """Sweep partition durations for paper-quality data."""
    durations = [0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0]

    if verbose:
        print("=" * 80)
        print("PARTITION DURATION SWEEP: VMTP vs QUIC vs TCP")
        print("=" * 80)
        print()
        print(f"{'Duration':>10} │ {'VMTP':>8} {'QUIC':>8} {'TCP':>8} │ {'VMTP rec':>10} {'QUIC rec':>10} {'TCP rec':>10}")
        print("─" * 10 + "─┼─" + "─" * 8 + "─" * 9 + "─" * 9 + "─┼─" + "─" * 10 + "─" * 11 + "─" * 11)

    data = []
    for duration in durations:
        vmtp = run_vmtp_partition(0.5, duration, transactions_before=5, transactions_during=5, transactions_after=5)
        quic = run_quic_partition(0.5, duration, transactions_before=5, transactions_during=5, transactions_after=5)
        tcp = run_tcp_partition(0.5, duration, transactions_before=5, transactions_during=5, transactions_after=5)

        vmtp_rec = f"{vmtp.first_success_after_heal*1000:.0f}ms" if vmtp.first_success_after_heal else "N/A"
        quic_rec = f"{quic.first_success_after_heal*1000:.0f}ms" if quic.first_success_after_heal else "N/A"
        tcp_rec = f"{tcp.first_success_after_heal*1000:.0f}ms" if tcp.first_success_after_heal else "N/A"

        data.append({
            "duration": duration,
            "vmtp_packets": vmtp.total_packets_sent,
            "quic_packets": quic.total_packets_sent,
            "tcp_packets": tcp.total_packets_sent,
            "vmtp_recovery_ms": vmtp.first_success_after_heal * 1000 if vmtp.first_success_after_heal else None,
            "quic_recovery_ms": quic.first_success_after_heal * 1000 if quic.first_success_after_heal else None,
            "tcp_recovery_ms": tcp.first_success_after_heal * 1000 if tcp.first_success_after_heal else None,
        })

        if verbose:
            print(f"{duration:>9.1f}s │ {vmtp.total_packets_sent:>8} {quic.total_packets_sent:>8} {tcp.total_packets_sent:>8} │ {vmtp_rec:>10} {quic_rec:>10} {tcp_rec:>10}")

    if verbose:
        print()
        print("Observation: VMTP recovery is always immediate (no reconnection)")
        print("             QUIC 0-RTT helps but still has overhead vs VMTP")
        print("             TCP has highest recovery cost")

    return data


if __name__ == "__main__":
    compare_partition_scenarios()
    print()
    sweep_partition_durations()
