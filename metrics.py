"""Metrics collection for VMTP simulation.

Tracks per-transaction and aggregate statistics.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum, auto


class TransactionResult(Enum):
    SUCCESS = auto()
    TIMEOUT = auto()
    ERROR = auto()


@dataclass
class TransactionMetrics:
    """Metrics for a single transaction."""

    client_id: int
    server_id: int
    transaction_id: int
    idempotent: bool

    # Timing
    start_time: float = 0.0
    end_time: Optional[float] = None

    # Packet counts
    requests_sent: int = 0
    responses_received: int = 0
    retransmits: int = 0

    # Outcome
    result: Optional[TransactionResult] = None
    duplicate_detected: bool = False  # Server detected this as duplicate
    duplicate_execution: bool = False  # Non-idempotent op executed twice (BUG!)

    @property
    def latency(self) -> Optional[float]:
        """Total time from first request to response."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return None

    @property
    def total_packets(self) -> int:
        """Total packets exchanged for this transaction."""
        return self.requests_sent + self.responses_received


class MetricsCollector:
    """Collects and aggregates simulation metrics."""

    def __init__(self):
        self._transactions: Dict[tuple, TransactionMetrics] = {}
        self._server_stats: Dict[int, dict] = {}

    def start_transaction(
        self,
        client_id: int,
        server_id: int,
        transaction_id: int,
        idempotent: bool,
        start_time: float,
    ) -> TransactionMetrics:
        """Record the start of a new transaction."""
        key = (client_id, transaction_id)
        metrics = TransactionMetrics(
            client_id=client_id,
            server_id=server_id,
            transaction_id=transaction_id,
            idempotent=idempotent,
            start_time=start_time,
        )
        self._transactions[key] = metrics
        return metrics

    def get_transaction(self, client_id: int, transaction_id: int) -> Optional[TransactionMetrics]:
        """Look up metrics for a transaction."""
        return self._transactions.get((client_id, transaction_id))

    def record_request_sent(self, client_id: int, transaction_id: int, is_retransmit: bool = False):
        """Record that a request was sent."""
        metrics = self.get_transaction(client_id, transaction_id)
        if metrics:
            metrics.requests_sent += 1
            if is_retransmit:
                metrics.retransmits += 1

    def record_response_received(self, client_id: int, transaction_id: int, end_time: float):
        """Record that a response was received."""
        metrics = self.get_transaction(client_id, transaction_id)
        if metrics:
            metrics.responses_received += 1
            if metrics.end_time is None:  # First response
                metrics.end_time = end_time
                metrics.result = TransactionResult.SUCCESS

    def record_timeout(self, client_id: int, transaction_id: int, end_time: float):
        """Record that a transaction timed out."""
        metrics = self.get_transaction(client_id, transaction_id)
        if metrics:
            metrics.end_time = end_time
            metrics.result = TransactionResult.TIMEOUT

    def record_duplicate_detected(self, client_id: int, transaction_id: int):
        """Record that the server detected a duplicate request."""
        metrics = self.get_transaction(client_id, transaction_id)
        if metrics:
            metrics.duplicate_detected = True

    def record_duplicate_execution(self, client_id: int, transaction_id: int):
        """Record that a non-idempotent operation was executed twice - this is a bug!"""
        metrics = self.get_transaction(client_id, transaction_id)
        if metrics:
            metrics.duplicate_execution = True

    def record_server_stats(self, server_id: int, stats: dict):
        """Record server-side statistics."""
        self._server_stats[server_id] = stats

    def summary(self) -> dict:
        """Generate summary statistics."""
        transactions = list(self._transactions.values())

        if not transactions:
            return {"total_transactions": 0}

        successful = [t for t in transactions if t.result == TransactionResult.SUCCESS]
        timed_out = [t for t in transactions if t.result == TransactionResult.TIMEOUT]
        with_retransmits = [t for t in transactions if t.retransmits > 0]
        duplicate_executions = [t for t in transactions if t.duplicate_execution]

        latencies = [t.latency for t in successful if t.latency is not None]
        packets = [t.total_packets for t in successful]

        return {
            "total_transactions": len(transactions),
            "successful": len(successful),
            "timed_out": len(timed_out),
            "success_rate": len(successful) / len(transactions) if transactions else 0,
            "with_retransmits": len(with_retransmits),
            "duplicate_executions": len(duplicate_executions),  # Should be 0!
            "avg_latency": sum(latencies) / len(latencies) if latencies else None,
            "min_latency": min(latencies) if latencies else None,
            "max_latency": max(latencies) if latencies else None,
            "avg_packets": sum(packets) / len(packets) if packets else None,
            "total_packets": sum(packets),
            "server_stats": self._server_stats,
        }

    @property
    def transactions(self) -> List[TransactionMetrics]:
        """Get all transaction metrics."""
        return list(self._transactions.values())
