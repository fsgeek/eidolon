"""VMTP Client protocol implementation.

Implements the client state machine with timeout and retry logic.
"""

import simpy
from dataclasses import dataclass
from typing import Optional, Generator, Any

from entity import Entity
from packet import Request, Response
from network import Network
from metrics import MetricsCollector


@dataclass
class TransactionConfig:
    """Configuration for transaction behavior."""

    timeout: float = 0.100  # TC1: Time to wait for response (100ms)
    max_retries: int = 5  # Maximum retransmission attempts
    idempotent: bool = True  # Whether the operation is idempotent


class Client:
    """VMTP Client protocol implementation.

    Runs as a SimPy process, sending requests and handling responses
    with timeout and retry logic.

    Uses a dispatcher pattern: a single receiver process pulls packets
    from the mailbox and routes them to waiting transactions via
    per-transaction events.
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        metrics: MetricsCollector,
        default_timeout: float = 0.100,
        default_max_retries: int = 5,
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.metrics = metrics
        self.default_timeout = default_timeout
        self.default_max_retries = default_max_retries

        # Transaction ID counter (monotonically increasing)
        self._next_transaction_id = 1

        # Pending response events: transaction_id -> simpy.Event
        # When a response arrives, we succeed the corresponding event
        self._pending: dict[int, simpy.Event] = {}
        self._responses: dict[int, Response] = {}

        # Statistics
        self._stats = {
            "transactions_started": 0,
            "transactions_completed": 0,
            "transactions_failed": 0,
            "requests_sent": 0,
            "retransmits": 0,
        }

        # Register with network and start receiver
        self.mailbox = network.register(entity.id)
        self.env.process(self._receiver())

    def transact(
        self,
        server_id: int,
        payload: Any = None,
        idempotent: bool = True,
        timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
    ) -> Generator[simpy.Event, Any, Optional[Response]]:
        """Execute a transaction with the given server.

        This is a SimPy process that should be yielded:
            response = yield from client.transact(server_id, payload)

        Returns the Response on success, None on timeout/failure.
        """
        timeout = timeout or self.default_timeout
        max_retries = max_retries if max_retries is not None else self.default_max_retries

        # Allocate transaction ID
        transaction_id = self._next_transaction_id
        self._next_transaction_id += 1

        self._stats["transactions_started"] += 1

        # Start metrics tracking
        self.metrics.start_transaction(
            client_id=self.entity.id,
            server_id=server_id,
            transaction_id=transaction_id,
            idempotent=idempotent,
            start_time=self.env.now,
        )

        # Create request
        request = Request(
            client_id=self.entity.id,
            server_id=server_id,
            transaction_id=transaction_id,
            idempotent=idempotent,
            payload=payload,
            timestamp=self.env.now,
        )

        # Try to complete the transaction with retries
        attempts = 0
        while attempts <= max_retries:
            is_retransmit = attempts > 0
            if is_retransmit:
                request.retransmit_count += 1
                self._stats["retransmits"] += 1

            # Send request
            self._stats["requests_sent"] += 1
            self.metrics.record_request_sent(
                self.entity.id, transaction_id, is_retransmit
            )
            self.network.send(request, server_id)

            # Wait for response with timeout
            response = yield self.env.process(
                self._wait_for_response(transaction_id, timeout)
            )

            if response is not None:
                # Success!
                self._stats["transactions_completed"] += 1
                self.metrics.record_response_received(
                    self.entity.id, transaction_id, self.env.now
                )
                return response

            # Timeout - retry
            attempts += 1

        # Exhausted retries
        self._stats["transactions_failed"] += 1
        self.metrics.record_timeout(self.entity.id, transaction_id, self.env.now)
        return None

    def _receiver(self):
        """Background process that receives packets and dispatches to waiters."""
        while True:
            packet = yield self.mailbox.get()

            if isinstance(packet, Response):
                txn_id = packet.transaction_id
                if txn_id in self._pending:
                    # Store response and signal waiter
                    self._responses[txn_id] = packet
                    self._pending[txn_id].succeed()
                # else: late response for completed/failed transaction, ignore

    def _wait_for_response(
        self, transaction_id: int, timeout: float
    ) -> Generator[simpy.Event, Any, Optional[Response]]:
        """Wait for a response to a specific transaction, with timeout."""
        # Create event for this transaction
        wait_event = self.env.event()
        self._pending[transaction_id] = wait_event

        # Race between response arriving and timeout
        timeout_event = self.env.timeout(timeout)
        result = yield wait_event | timeout_event

        # Clean up
        self._pending.pop(transaction_id, None)

        if wait_event in result:
            # Got response
            return self._responses.pop(transaction_id, None)
        else:
            # Timeout
            return None

    @property
    def stats(self) -> dict:
        """Return client statistics."""
        return self._stats.copy()
