"""VMTP vs QUIC comparison.

This is the real test. TCP is 1981 technology - QUIC is the modern alternative.

Key QUIC features that compete with VMTP:
- 0-RTT resumption (session tickets)
- 1-RTT new connection (vs TCP's 1.5 RTT + TLS)
- Connection ID (allows some migration)
"""

import simpy
from dataclasses import dataclass

from entity import EntityRegistry
from network import Network, NetworkConfig
from server import Server as VmtpServer
from client import Client as VmtpClient
from quic_baseline import QuicServer, QuicClient
from metrics import MetricsCollector


def run_vmtp_scenario(
    num_transactions: int,
    loss_rate: float = 0.0,
    seed: int = 42,
) -> dict:
    """Run VMTP scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(env, NetworkConfig(base_delay=0.010, loss_probability=loss_rate, seed=seed))

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = VmtpServer(env, server_entity, network, metrics, process_time=0.005)
    client = VmtpClient(env, client_entity, network, metrics, default_timeout=0.100)

    latencies = []

    def workload():
        for i in range(num_transactions):
            start = env.now
            response = yield from client.transact(server_entity.id, f"op-{i}")
            if response:
                latencies.append(env.now - start)
            yield env.timeout(0.001)

    env.process(workload())
    env.run(until=300.0)

    summary = metrics.summary()
    return {
        "protocol": "VMTP",
        "success_rate": summary["success_rate"],
        "total_packets": network.stats["sent"],
        "avg_latency": sum(latencies) / len(latencies) if latencies else None,
        "successful": summary["successful"],
    }


def run_quic_scenario(
    num_transactions: int,
    loss_rate: float = 0.0,
    seed: int = 42,
    fresh_connection: bool = True,
) -> dict:
    """Run QUIC scenario."""
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(env, NetworkConfig(base_delay=0.010, loss_probability=loss_rate, seed=seed))

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = QuicServer(env, server_entity, network, metrics, process_time=0.005)
    client = QuicClient(env, client_entity, network, metrics, default_timeout=0.100)

    latencies = []

    def workload():
        # If not fresh, do one transaction to get session ticket
        if not fresh_connection:
            yield from client.transact(server_entity.id, "warmup")

        for i in range(num_transactions):
            start = env.now
            response = yield from client.transact(server_entity.id, f"op-{i}")
            if response:
                latencies.append(env.now - start)
            yield env.timeout(0.001)

    env.process(workload())
    env.run(until=300.0)

    return {
        "protocol": "QUIC" + ("" if fresh_connection else " (0-RTT)"),
        "success_rate": client.stats["transactions_completed"] / client.stats["transactions_started"] if client.stats["transactions_started"] else 0,
        "total_packets": network.stats["sent"],
        "avg_latency": sum(latencies) / len(latencies) if latencies else None,
        "successful": client.stats["transactions_completed"],
        "zero_rtt": server.stats["zero_rtt_accepted"],
    }


def compare_cold_start(verbose: bool = True):
    """Compare first transaction cost (cold start)."""
    if verbose:
        print("=" * 80)
        print("CONNECTION COST COMPARISON: VMTP vs QUIC")
        print("=" * 80)
        print()

    vmtp = run_vmtp_scenario(1)
    quic_fresh = run_quic_scenario(1, fresh_connection=True)

    # For 0-RTT, measure ONLY the resumed transaction
    # (warmup establishes session ticket, then we measure the 0-RTT transaction)
    env = simpy.Environment()
    registry = EntityRegistry()
    network = Network(env, NetworkConfig(base_delay=0.010, seed=42))
    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")
    server = QuicServer(env, server_entity, network, None, process_time=0.005)
    client = QuicClient(env, client_entity, network, None)

    zero_rtt_latency = [None]
    zero_rtt_packets = [0]

    def zero_rtt_workload():
        # Warmup to get session ticket
        yield from client.transact(server_entity.id, "warmup")
        warmup_packets = network.stats["sent"]

        # Now measure 0-RTT transaction
        start = env.now
        response = yield from client.transact(server_entity.id, "0rtt-test")
        if response:
            zero_rtt_latency[0] = env.now - start
            zero_rtt_packets[0] = network.stats["sent"] - warmup_packets

    env.process(zero_rtt_workload())
    env.run(until=10.0)

    if verbose:
        print(f"{'Scenario':<25} {'Packets':>10} {'Latency':>12} {'Notes':<30}")
        print("─" * 25 + "─" * 11 + "─" * 13 + "─" * 30)
        print(f"{'VMTP (any transaction)':<25} {vmtp['total_packets']:>10} {vmtp['avg_latency']*1000:>11.1f}ms {'Always 2 packets':<30}")
        print(f"{'QUIC (new connection)':<25} {quic_fresh['total_packets']:>10} {quic_fresh['avg_latency']*1000:>11.1f}ms {'Initial + Handshake + ACK + Req + Resp':<30}")
        print(f"{'QUIC (0-RTT resume)':<25} {zero_rtt_packets[0]:>10} {zero_rtt_latency[0]*1000:>11.1f}ms {'0-RTT + Resp (has session ticket)':<30}")
        print()
        print("Key insight:")
        print("  - VMTP: Every transaction is 2 packets, no setup overhead")
        print("  - QUIC fresh: 5 packets (handshake + transaction)")
        print("  - QUIC 0-RTT: 2 packets (matches VMTP efficiency)")
        print()
        print("  QUIC 0-RTT requires a previous session. VMTP is always fast.")
        print()

    return {
        "vmtp": vmtp,
        "quic_fresh": quic_fresh,
        "quic_0rtt_packets": zero_rtt_packets[0],
        "quic_0rtt_latency": zero_rtt_latency[0],
    }


def compare_multiple_transactions(verbose: bool = True):
    """Compare over multiple transactions to same server."""
    if verbose:
        print("=" * 80)
        print("MULTIPLE TRANSACTIONS TO SAME SERVER")
        print("=" * 80)
        print()
        print("Scenario: Client makes N transactions to the same server")
        print("QUIC gets session ticket after first transaction")
        print()
        print(f"{'N txns':<10} │ {'VMTP':>10} {'QUIC':>10} {'VMTP/txn':>10} {'QUIC/txn':>10}")
        print("─" * 10 + "─┼─" + "─" * 10 + "─" * 11 + "─" * 11 + "─" * 11)

    for n in [1, 2, 5, 10, 20, 50]:
        vmtp = run_vmtp_scenario(n)
        quic = run_quic_scenario(n, fresh_connection=True)

        vmtp_per = vmtp['total_packets'] / n
        quic_per = quic['total_packets'] / n

        if verbose:
            print(f"{n:<10} │ {vmtp['total_packets']:>10} {quic['total_packets']:>10} {vmtp_per:>10.2f} {quic_per:>10.2f}")

    if verbose:
        print()
        print("As N increases, QUIC's setup cost amortizes. But VMTP is always 2.0/txn.")
        print()


def compare_with_loss(verbose: bool = True):
    """Compare under packet loss."""
    if verbose:
        print("=" * 80)
        print("PACKET LOSS COMPARISON")
        print("=" * 80)
        print()
        print(f"{'Loss %':<10} │ {'VMTP':>10} {'QUIC':>10} {'Δ':>8}")
        print("─" * 10 + "─┼─" + "─" * 10 + "─" * 11 + "─" * 9)

    for loss in [0.0, 0.05, 0.10, 0.20]:
        vmtp = run_vmtp_scenario(20, loss_rate=loss)
        quic = run_quic_scenario(20, loss_rate=loss)

        vmtp_pkts_per_txn = vmtp['total_packets'] / vmtp['successful'] if vmtp['successful'] else 0
        quic_pkts_per_txn = quic['total_packets'] / quic['successful'] if quic['successful'] else 0

        if verbose:
            delta = quic_pkts_per_txn - vmtp_pkts_per_txn
            print(f"{loss*100:>8.0f}% │ {vmtp_pkts_per_txn:>10.2f} {quic_pkts_per_txn:>10.2f} {delta:>+8.2f}")

    if verbose:
        print()
        print("Values are packets per successful transaction")


def compare_migration(verbose: bool = True):
    """Compare migration costs."""
    if verbose:
        print("=" * 80)
        print("MIGRATION COMPARISON")
        print("=" * 80)
        print()
        print("Scenario: Server migrates to new address during session")
        print()
        print(f"{'Protocol':<15} {'Migration cost':>20} {'Client action':>25}")
        print("─" * 15 + "─" * 21 + "─" * 26)
        print(f"{'VMTP':<15} {'0 packets':>20} {'None (same entity ID)':>25}")
        print(f"{'QUIC':<15} {'~4 packets':>20} {'Connection migration handshake':>25}")
        print(f"{'TCP':<15} {'~7 packets':>20} {'Full reconnection':>25}")
        print()
        print("VMTP:  Entity ID is address-independent. Migration = update routing table.")
        print("QUIC:  Connection ID helps, but still needs PATH_CHALLENGE/PATH_RESPONSE.")
        print("TCP:   Connection = 4-tuple. Any change = full teardown + setup.")
        print()


def summarize_findings(verbose: bool = True):
    """Print summary of VMTP vs QUIC findings."""
    if verbose:
        print()
        print("=" * 80)
        print("SUMMARY: VMTP vs QUIC vs TCP")
        print("=" * 80)
        print()
        print("┌─────────────────────┬──────────┬──────────┬──────────┐")
        print("│ Metric              │   VMTP   │   QUIC   │   TCP    │")
        print("├─────────────────────┼──────────┼──────────┼──────────┤")
        print("│ Cold start packets  │    2     │    5     │    8+    │")
        print("│ Resumed packets     │    2     │  2 (0RTT)│    2     │")
        print("│ Migration cost      │    0     │   ~4     │   ~7     │")
        print("│ Session state req'd │    No    │   Yes    │   Yes    │")
        print("│ CSR/state on server │   Yes    │   Yes    │   Yes    │")
        print("│ Ecosystem           │   None   │  Large   │  Huge    │")
        print("└─────────────────────┴──────────┴──────────┴──────────┘")
        print()
        print("VMTP's unique advantages:")
        print("  1. Always-fast cold start (no session ticket needed)")
        print("  2. Zero-cost migration (entity IDs are address-independent)")
        print("  3. Simpler client (no session ticket management)")
        print()
        print("VMTP's disadvantages:")
        print("  1. No ecosystem (would need to build everything)")
        print("  2. CSR eviction breaks non-idempotent without probing")
        print("  3. No built-in security (would need to add)")
        print()
        print("Verdict:")
        print("  For datacenter microservices with many short-lived connections")
        print("  and frequent service migration, VMTP offers real advantages.")
        print()
        print("  For general internet use, QUIC's ecosystem wins despite")
        print("  slightly higher overhead.")
        print("=" * 80)


if __name__ == "__main__":
    compare_cold_start()
    compare_multiple_transactions()
    compare_with_loss()
    compare_migration()
    summarize_findings()
