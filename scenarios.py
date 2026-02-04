"""Configurable test scenarios for VMTP simulation.

Each scenario defines network conditions and workload parameters.
"""

import simpy
from dataclasses import dataclass, field
from typing import Callable, Any

from entity import EntityRegistry
from network import Network, NetworkConfig
from server import Server
from client import Client
from metrics import MetricsCollector, TransactionResult


@dataclass
class ScenarioConfig:
    """Configuration for a test scenario."""

    name: str
    description: str = ""

    # Network parameters
    base_delay: float = 0.010  # 10ms one-way
    delay_jitter: float = 0.002
    loss_probability: float = 0.0
    seed: int | None = None

    # Server parameters
    process_time: float = 0.005  # 5ms
    csr_capacity: int = 1000

    # Client parameters
    timeout: float = 0.050  # 50ms
    max_retries: int = 5

    # Workload parameters
    num_transactions: int = 50
    idempotent_ratio: float = 0.5  # Fraction of ops that are idempotent
    inter_transaction_delay: float = 0.001  # 1ms between transactions


@dataclass
class ScenarioResult:
    """Results from running a scenario."""

    config: ScenarioConfig
    metrics_summary: dict
    network_stats: dict
    server_stats: dict
    client_stats: dict

    @property
    def passed(self) -> bool:
        """Check if scenario passed basic correctness checks."""
        # Key invariant: no duplicate executions
        return self.metrics_summary.get("duplicate_executions", 0) == 0


def run_scenario(config: ScenarioConfig) -> ScenarioResult:
    """Run a scenario and return results."""
    env = simpy.Environment()
    registry = EntityRegistry()
    metrics = MetricsCollector()

    network = Network(
        env,
        NetworkConfig(
            base_delay=config.base_delay,
            delay_jitter=config.delay_jitter,
            loss_probability=config.loss_probability,
            seed=config.seed,
        ),
    )

    server_entity = registry.create(name="server")
    client_entity = registry.create(name="client")

    server = Server(
        env,
        server_entity,
        network,
        metrics,
        process_time=config.process_time,
        csr_capacity=config.csr_capacity,
    )

    client = Client(
        env,
        client_entity,
        network,
        metrics,
        default_timeout=config.timeout,
        default_max_retries=config.max_retries,
    )

    def workload():
        for i in range(config.num_transactions):
            idempotent = (i / config.num_transactions) < config.idempotent_ratio
            yield from client.transact(
                server_id=server_entity.id,
                payload=f"op-{i}",
                idempotent=idempotent,
            )
            yield env.timeout(config.inter_transaction_delay)

    env.process(workload())
    env.run(until=300.0)  # 5 minute max

    return ScenarioResult(
        config=config,
        metrics_summary=metrics.summary(),
        network_stats=network.stats,
        server_stats=server.stats,
        client_stats=client.stats,
    )


# Pre-defined scenarios

HAPPY_PATH = ScenarioConfig(
    name="happy_path",
    description="No packet loss, baseline performance",
    loss_probability=0.0,
    num_transactions=10,
)

LIGHT_LOSS = ScenarioConfig(
    name="light_loss",
    description="5% packet loss - occasional retransmits",
    loss_probability=0.05,
    num_transactions=100,
    seed=42,
)

MODERATE_LOSS = ScenarioConfig(
    name="moderate_loss",
    description="20% packet loss - frequent retransmits",
    loss_probability=0.20,
    num_transactions=100,
    seed=42,
)

HEAVY_LOSS = ScenarioConfig(
    name="heavy_loss",
    description="40% packet loss - stress test retry logic",
    loss_probability=0.40,
    num_transactions=50,
    seed=42,
)

NON_IDEMPOTENT_ONLY = ScenarioConfig(
    name="non_idempotent_only",
    description="All non-idempotent ops - test CSR caching",
    loss_probability=0.20,
    num_transactions=50,
    idempotent_ratio=0.0,
    seed=42,
)

HIGH_LATENCY = ScenarioConfig(
    name="high_latency",
    description="100ms network delay - test timeout handling",
    base_delay=0.100,
    timeout=0.300,  # Need longer timeout for higher latency
    num_transactions=20,
)

ALL_SCENARIOS = [
    HAPPY_PATH,
    LIGHT_LOSS,
    MODERATE_LOSS,
    HEAVY_LOSS,
    NON_IDEMPOTENT_ONLY,
    HIGH_LATENCY,
]


def run_all_scenarios(verbose: bool = True) -> list[ScenarioResult]:
    """Run all predefined scenarios."""
    results = []

    for config in ALL_SCENARIOS:
        if verbose:
            print(f"\n{'='*60}")
            print(f"SCENARIO: {config.name}")
            print(f"  {config.description}")
            print(f"{'='*60}")

        result = run_scenario(config)
        results.append(result)

        if verbose:
            s = result.metrics_summary
            print(f"  Success rate: {s['success_rate']*100:.1f}%")
            print(f"  Avg latency: {s['avg_latency']*1000:.1f}ms" if s['avg_latency'] else "  N/A")
            print(f"  Avg packets/txn: {s['avg_packets']:.2f}" if s['avg_packets'] else "  N/A")
            print(f"  Duplicate executions: {s['duplicate_executions']}")
            status = "✓ PASS" if result.passed else "✗ FAIL"
            print(f"  Status: {status}")

    return results


if __name__ == "__main__":
    results = run_all_scenarios()
    print("\n" + "=" * 60)
    passed = sum(1 for r in results if r.passed)
    print(f"SUMMARY: {passed}/{len(results)} scenarios passed")
