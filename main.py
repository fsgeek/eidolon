"""VMTP Simulator - main entry point.

Run simulations of VMTP transaction protocol under various conditions.
"""

import simpy
from entity import EntityRegistry
from packet import Request, Response
from network import Network, NetworkConfig
from server import Server
from client import Client
from metrics import MetricsCollector


def run_happy_path():
    """Phase 1: Single transaction, no packet loss.

    Expected: 2 packets (1 request, 1 response), ~RTT latency.
    """
    print("=" * 60)
    print("HAPPY PATH: Single transaction, no loss")
    print("=" * 60)

    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(env, NetworkConfig(
        base_delay=0.010,  # 10ms one-way
        loss_probability=0.0,
        seed=42,
    ))

    # Create server and client entities
    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    # Create protocol instances
    server = Server(env, server_entity, network, metrics, process_time=0.005)
    client = Client(env, client_entity, network, metrics)

    # Run a single transaction
    def client_workload():
        response = yield from client.transact(
            server_id=server_entity.id,
            payload="hello",
            idempotent=True,
        )
        if response:
            print(f"  Transaction completed: {response}")
        else:
            print("  Transaction FAILED")

    env.process(client_workload())
    env.run(until=1.0)  # Run for 1 second

    # Report results
    summary = metrics.summary()
    print(f"\nResults:")
    print(f"  Transactions: {summary['successful']}/{summary['total_transactions']} successful")
    print(f"  Total packets: {summary['total_packets']}")
    print(f"  Avg latency: {summary['avg_latency']*1000:.1f}ms" if summary['avg_latency'] else "  No latency data")
    print(f"  Network: {network.stats}")
    print()


def run_packet_loss(loss_rate: float = 0.1, num_transactions: int = 100):
    """Phase 2: Transactions under packet loss.

    Tests timeout/retry behavior and duplicate detection.
    """
    print("=" * 60)
    print(f"PACKET LOSS: {loss_rate*100:.0f}% loss, {num_transactions} transactions")
    print("=" * 60)

    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()
    network = Network(env, NetworkConfig(
        base_delay=0.010,
        loss_probability=loss_rate,
        seed=42,
    ))

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = Server(env, server_entity, network, metrics, process_time=0.005)
    client = Client(env, client_entity, network, metrics, default_timeout=0.050)

    def client_workload():
        for i in range(num_transactions):
            # Alternate between idempotent and non-idempotent
            is_idempotent = (i % 2 == 0)

            response = yield from client.transact(
                server_id=server_entity.id,
                payload=f"request-{i}",
                idempotent=is_idempotent,
            )

            # Small delay between transactions
            yield env.timeout(0.001)

    env.process(client_workload())
    env.run(until=60.0)  # Run for up to 60 seconds

    # Report results
    summary = metrics.summary()
    print(f"\nResults:")
    print(f"  Success rate: {summary['success_rate']*100:.1f}%")
    print(f"  Transactions: {summary['successful']}/{summary['total_transactions']} successful")
    print(f"  Timed out: {summary['timed_out']}")
    print(f"  With retransmits: {summary['with_retransmits']}")
    print(f"  Total packets: {summary['total_packets']}")
    if summary['avg_latency']:
        print(f"  Avg latency: {summary['avg_latency']*1000:.1f}ms")
        print(f"  Min latency: {summary['min_latency']*1000:.1f}ms")
        print(f"  Max latency: {summary['max_latency']*1000:.1f}ms")
    print(f"  Avg packets/txn: {summary['avg_packets']:.2f}" if summary['avg_packets'] else "")
    print(f"  DUPLICATE EXECUTIONS: {summary['duplicate_executions']}")  # Should be 0!
    print(f"  Network: {network.stats}")
    print(f"  Server: {server.stats}")
    print(f"  Client: {client.stats}")
    print()


def run_stress_test():
    """Run at various loss rates to see behavior."""
    print("=" * 60)
    print("STRESS TEST: Varying loss rates")
    print("=" * 60)
    print()

    for loss_rate in [0.0, 0.05, 0.10, 0.20, 0.30, 0.50]:
        env = simpy.Environment()
        registry = EntityRegistry()
        metrics = MetricsCollector()
        network = Network(env, NetworkConfig(
            base_delay=0.010,
            loss_probability=loss_rate,
            seed=42,
        ))

        server_entity = registry.create(name="server")
        client_entity = registry.create(name="client")

        server = Server(env, server_entity, network, metrics, process_time=0.005)
        client = Client(env, client_entity, network, metrics, default_timeout=0.050)

        def client_workload():
            for i in range(50):
                yield from client.transact(
                    server_id=server_entity.id,
                    payload=f"request-{i}",
                    idempotent=(i % 2 == 0),
                )
                yield env.timeout(0.001)

        env.process(client_workload())
        env.run(until=60.0)

        summary = metrics.summary()
        avg_pkts = summary['avg_packets'] or 0
        print(f"  {loss_rate*100:4.0f}% loss: "
              f"{summary['success_rate']*100:5.1f}% success, "
              f"{avg_pkts:.1f} pkts/txn, "
              f"dup_exec={summary['duplicate_executions']}")

    print()


def main():
    """Run all simulations."""
    run_happy_path()
    run_packet_loss(loss_rate=0.10, num_transactions=50)
    run_stress_test()


if __name__ == "__main__":
    main()
