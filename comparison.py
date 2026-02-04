"""Head-to-head comparison of VMTP vs TCP baseline.

Runs identical scenarios against both protocols and compares:
- Packets per transaction
- Latency
- Success rate under failure
- Duplicate execution (correctness)
"""

import simpy
from dataclasses import dataclass

from entity import EntityRegistry
from network import Network, NetworkConfig
from server import Server as VmtpServer
from client import Client as VmtpClient
from tcp_baseline import TcpServer, TcpClient
from metrics import MetricsCollector


@dataclass
class ComparisonResult:
    """Results from comparing VMTP and TCP on the same scenario."""

    scenario_name: str
    loss_rate: float

    # VMTP results
    vmtp_success_rate: float
    vmtp_avg_packets: float
    vmtp_avg_latency: float | None
    vmtp_dup_executions: int

    # TCP results
    tcp_success_rate: float
    tcp_avg_packets: float
    tcp_avg_latency: float | None
    tcp_dup_executions: int

    # Comparison
    @property
    def packet_ratio(self) -> float | None:
        """TCP packets / VMTP packets (>1 means TCP uses more)."""
        if self.vmtp_avg_packets and self.tcp_avg_packets:
            return self.tcp_avg_packets / self.vmtp_avg_packets
        return None

    @property
    def latency_ratio(self) -> float | None:
        """TCP latency / VMTP latency."""
        if self.vmtp_avg_latency and self.tcp_avg_latency:
            return self.tcp_avg_latency / self.vmtp_avg_latency
        return None


def run_vmtp_scenario(
    loss_rate: float,
    num_transactions: int,
    seed: int,
    idempotent_ratio: float = 0.5,
) -> dict:
    """Run VMTP through a scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=loss_rate, seed=seed),
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = VmtpServer(env, server_entity, network, metrics, process_time=0.005)
    client = VmtpClient(env, client_entity, network, metrics, default_timeout=0.050)

    def workload():
        for i in range(num_transactions):
            idempotent = (i / num_transactions) < idempotent_ratio
            yield from client.transact(
                server_id=server_entity.id,
                payload=f"op-{i}",
                idempotent=idempotent,
            )
            yield env.timeout(0.001)

    env.process(workload())
    env.run(until=300.0)

    summary = metrics.summary()
    return {
        "success_rate": summary["success_rate"],
        "avg_packets": summary["avg_packets"],
        "avg_latency": summary["avg_latency"],
        "dup_executions": summary["duplicate_executions"],
        "network_stats": network.stats,
        "server_stats": server.stats,
        "client_stats": client.stats,
    }


def run_tcp_scenario(
    loss_rate: float,
    num_transactions: int,
    seed: int,
    idempotent_ratio: float = 0.5,
) -> dict:
    """Run TCP baseline through a scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = Network(
        env,
        NetworkConfig(base_delay=0.010, loss_probability=loss_rate, seed=seed),
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    # We need to track metrics manually for TCP
    server = TcpServer(env, server_entity, network, None, process_time=0.005)
    client = TcpClient(env, client_entity, network, None, default_timeout=0.050)

    # Track transaction timing manually
    transaction_times = []
    successes = 0
    failures = 0

    def workload():
        nonlocal successes, failures
        for i in range(num_transactions):
            idempotent = (i / num_transactions) < idempotent_ratio
            start = env.now
            response = yield from client.transact(
                server_id=server_entity.id,
                payload=f"op-{i}",
                idempotent=idempotent,
            )
            if response:
                successes += 1
                transaction_times.append(env.now - start)
            else:
                failures += 1
            yield env.timeout(0.001)

    env.process(workload())
    env.run(until=300.0)

    total = successes + failures
    client_stats = client.stats
    server_stats = server.stats

    # Calculate packets per successful transaction
    # For TCP: connection packets + request/response packets
    total_packets = client_stats["packets_sent"] + server_stats["packets_sent"]
    avg_packets = total_packets / successes if successes > 0 else None

    return {
        "success_rate": successes / total if total > 0 else 0,
        "avg_packets": avg_packets,
        "avg_latency": sum(transaction_times) / len(transaction_times) if transaction_times else None,
        "dup_executions": server_stats["duplicate_executions"],
        "network_stats": network.stats,
        "server_stats": server_stats,
        "client_stats": client_stats,
    }


def compare(
    scenario_name: str,
    loss_rate: float,
    num_transactions: int = 50,
    seed: int = 42,
    idempotent_ratio: float = 0.5,
) -> ComparisonResult:
    """Run both protocols through the same scenario and compare."""
    vmtp = run_vmtp_scenario(loss_rate, num_transactions, seed, idempotent_ratio)
    tcp = run_tcp_scenario(loss_rate, num_transactions, seed, idempotent_ratio)

    return ComparisonResult(
        scenario_name=scenario_name,
        loss_rate=loss_rate,
        vmtp_success_rate=vmtp["success_rate"],
        vmtp_avg_packets=vmtp["avg_packets"] or 0,
        vmtp_avg_latency=vmtp["avg_latency"],
        vmtp_dup_executions=vmtp["dup_executions"],
        tcp_success_rate=tcp["success_rate"],
        tcp_avg_packets=tcp["avg_packets"] or 0,
        tcp_avg_latency=tcp["avg_latency"],
        tcp_dup_executions=tcp["dup_executions"],
    )


def run_comparison_suite(verbose: bool = True) -> list[ComparisonResult]:
    """Run full comparison suite."""
    scenarios = [
        ("no_loss", 0.00),
        ("light_loss_5%", 0.05),
        ("moderate_loss_10%", 0.10),
        ("heavy_loss_20%", 0.20),
        ("severe_loss_30%", 0.30),
    ]

    results = []

    if verbose:
        print("=" * 80)
        print("VMTP vs TCP BASELINE COMPARISON")
        print("=" * 80)
        print()
        print(f"{'Scenario':<20} {'Loss':>6} │ {'VMTP pkts':>10} {'TCP pkts':>10} {'Ratio':>7} │ {'VMTP ok':>7} {'TCP ok':>7}")
        print("─" * 20 + "─" * 7 + "─┼─" + "─" * 10 + "─" * 11 + "─" * 8 + "─┼─" + "─" * 7 + "─" * 8)

    for name, loss_rate in scenarios:
        result = compare(name, loss_rate)
        results.append(result)

        if verbose:
            ratio = f"{result.packet_ratio:.2f}x" if result.packet_ratio else "N/A"
            vmtp_ok = f"{result.vmtp_success_rate*100:.0f}%"
            tcp_ok = f"{result.tcp_success_rate*100:.0f}%"
            print(
                f"{name:<20} {loss_rate*100:>5.0f}% │ "
                f"{result.vmtp_avg_packets:>10.2f} {result.tcp_avg_packets:>10.2f} {ratio:>7} │ "
                f"{vmtp_ok:>7} {tcp_ok:>7}"
            )

    if verbose:
        print()
        print("Ratio > 1.0 means TCP uses more packets per transaction than VMTP")
        print()

        # Check for duplicate handling
        vmtp_dups = sum(r.vmtp_dup_executions for r in results)
        tcp_dups = sum(r.tcp_dup_executions for r in results)

        print(f"VMTP duplicate executions: {vmtp_dups}")
        print(f"TCP duplicates requiring app-level dedup: {tcp_dups}")
        print()
        if vmtp_dups == 0:
            print("✓ VMTP: CSR provides transport-level duplicate suppression")
        else:
            print("✗ VMTP: Duplicate execution detected (bug!)")

        if tcp_dups > 0:
            print(f"! TCP: {tcp_dups} cases where app-level idempotency handling needed")
            print("  (TCP doesn't provide CSR-equivalent at transport layer)")

    return results


def compare_first_transaction(verbose: bool = True) -> dict:
    """Compare the cost of a single transaction with no prior connection.

    This is where VMTP should shine - no connection setup overhead.
    """
    if verbose:
        print("=" * 80)
        print("FIRST TRANSACTION COMPARISON (cold start)")
        print("=" * 80)
        print()

    # VMTP: single transaction
    vmtp = run_vmtp_scenario(loss_rate=0.0, num_transactions=1, seed=42)

    # TCP: single transaction (includes connection setup)
    tcp = run_tcp_scenario(loss_rate=0.0, num_transactions=1, seed=42)

    if verbose:
        print(f"VMTP: {vmtp['avg_packets']:.0f} packets, {vmtp['avg_latency']*1000:.1f}ms latency")
        print(f"TCP:  {tcp['avg_packets']:.0f} packets, {tcp['avg_latency']*1000:.1f}ms latency")
        print()
        packet_diff = tcp['avg_packets'] - vmtp['avg_packets']
        latency_diff = (tcp['avg_latency'] - vmtp['avg_latency']) * 1000
        print(f"TCP overhead: +{packet_diff:.0f} packets, +{latency_diff:.1f}ms")
        print()
        print("VMTP: Request → Response (2 packets)")
        print("TCP:  SYN → SYN-ACK → ACK → Request → Response (5 packets)")

    return {"vmtp": vmtp, "tcp": tcp}


def compare_amortization(verbose: bool = True) -> dict:
    """Show how TCP's connection cost amortizes over multiple transactions.

    VMTP should win for few transactions, TCP catches up with many.
    """
    if verbose:
        print("=" * 80)
        print("AMORTIZATION: TCP connection cost over N transactions (0% loss)")
        print("=" * 80)
        print()
        print(f"{'Transactions':>12} │ {'VMTP pkts':>10} {'TCP pkts':>10} {'TCP/VMTP':>10}")
        print("─" * 12 + "─┼─" + "─" * 10 + "─" * 11 + "─" * 11)

    results = {}
    for n in [1, 2, 5, 10, 20, 50, 100]:
        vmtp = run_vmtp_scenario(loss_rate=0.0, num_transactions=n, seed=42)
        tcp = run_tcp_scenario(loss_rate=0.0, num_transactions=n, seed=42)

        # Total packets, not per-transaction
        vmtp_total = vmtp['avg_packets'] * n if vmtp['avg_packets'] else 0
        tcp_total = tcp['avg_packets'] * n if tcp['avg_packets'] else 0

        results[n] = {"vmtp": vmtp_total, "tcp": tcp_total}

        if verbose:
            ratio = tcp_total / vmtp_total if vmtp_total else 0
            print(f"{n:>12} │ {vmtp_total:>10.0f} {tcp_total:>10.0f} {ratio:>10.2f}x")

    if verbose:
        print()
        print("Note: TCP's 3-packet handshake amortizes over many transactions")
        print("      VMTP wins more decisively for short-lived interactions")

    return results


if __name__ == "__main__":
    compare_first_transaction()
    print()
    compare_amortization()
    print()
    run_comparison_suite()
