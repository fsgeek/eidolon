"""VMTP Server protocol implementation.

Implements the server state machine with Client State Record (CSR)
management for duplicate detection and response caching.
"""

import simpy
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, Any
from collections import OrderedDict

from entity import Entity
from packet import Request, Response
from network import Network
from metrics import MetricsCollector


@dataclass
class ClientStateRecord:
    """Server-side state about a remote client.

    Key insight from VMTP: servers CAN discard CSRs at any time
    without affecting correctness - it just means the next request
    triggers a probe to resync.
    """

    client_id: int
    last_transaction_id: int
    cached_response: Optional[Response] = None
    last_activity: float = 0.0
    executions: int = 0  # Track how many times we've executed for this client


class CSRCache:
    """LRU cache for Client State Records.

    Implements eviction when capacity is reached.
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: OrderedDict[int, ClientStateRecord] = OrderedDict()

    def get(self, client_id: int) -> Optional[ClientStateRecord]:
        """Get CSR for a client, updating LRU order."""
        if client_id in self._cache:
            self._cache.move_to_end(client_id)
            return self._cache[client_id]
        return None

    def put(self, csr: ClientStateRecord):
        """Store or update a CSR."""
        if csr.client_id in self._cache:
            self._cache.move_to_end(csr.client_id)
        else:
            if len(self._cache) >= self.max_size:
                # Evict oldest (least recently used)
                self._cache.popitem(last=False)
        self._cache[csr.client_id] = csr

    def evict(self, client_id: int):
        """Explicitly evict a CSR."""
        self._cache.pop(client_id, None)

    def __len__(self):
        return len(self._cache)


class Server:
    """VMTP Server protocol implementation.

    Runs as a SimPy process, receiving requests from its mailbox
    and sending responses through the network.
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        metrics: MetricsCollector,
        process_time: float = 0.005,  # 5ms default processing time
        csr_capacity: int = 1000,
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.metrics = metrics
        self.process_time = process_time
        self.csr_cache = CSRCache(max_size=csr_capacity)

        # Ground truth: track ALL executions regardless of CSR state
        # Used to detect when CSR eviction causes silent re-execution
        self._execution_log: dict[tuple[int, int], int] = {}  # (client, txn) -> exec count

        # Statistics
        self._stats = {
            "requests_received": 0,
            "responses_sent": 0,
            "duplicates_detected": 0,
            "cache_hits": 0,
            "executions": 0,
            "silent_reexecutions": 0,  # Re-executions due to CSR eviction
        }

        # Register with network and start processing
        self.mailbox = network.register(entity.id)
        self.process = env.process(self._run())

    def _run(self):
        """Main server loop - receive requests and process them."""
        while True:
            # Wait for a request
            packet = yield self.mailbox.get()

            if not isinstance(packet, Request):
                continue  # Ignore non-requests

            self._stats["requests_received"] += 1
            yield self.env.process(self._handle_request(packet))

    def _handle_request(self, request: Request):
        """Handle an incoming request with duplicate detection."""
        csr = self.csr_cache.get(request.client_id)

        if csr is not None:
            # We have state for this client
            if request.transaction_id == csr.last_transaction_id:
                # Duplicate request
                self._stats["duplicates_detected"] += 1
                self.metrics.record_duplicate_detected(
                    request.client_id, request.transaction_id
                )

                if csr.cached_response is not None:
                    # Resend cached response
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
                elif request.idempotent:
                    # Idempotent op, can safely re-execute
                    pass  # Fall through to execution
                else:
                    # Non-idempotent with no cached response - problem!
                    # In real VMTP this triggers a probe, for now we'll re-execute
                    # and flag it as a duplicate execution (a bug condition)
                    self.metrics.record_duplicate_execution(
                        request.client_id, request.transaction_id
                    )

            elif request.transaction_id < csr.last_transaction_id:
                # Old duplicate - silently discard
                return

        # New request (or acceptable retry) - process it
        yield self.env.process(self._execute(request))

    def _execute(self, request: Request):
        """Execute the operation and send response."""
        key = (request.client_id, request.transaction_id)

        # Track ground truth execution count
        prev_executions = self._execution_log.get(key, 0)
        self._execution_log[key] = prev_executions + 1

        if prev_executions > 0:
            # We've executed this before - CSR must have been evicted
            self._stats["silent_reexecutions"] += 1
            if not request.idempotent:
                # This is the dangerous case: non-idempotent op re-executed
                # because CSR was evicted. In real VMTP, we'd probe the client.
                self.metrics.record_duplicate_execution(
                    request.client_id, request.transaction_id
                )

        # Simulate processing time
        yield self.env.timeout(self.process_time)
        self._stats["executions"] += 1

        # Create response
        response = Response(
            client_id=request.client_id,
            server_id=self.entity.id,
            transaction_id=request.transaction_id,
            payload=f"result-{request.transaction_id}",
            timestamp=self.env.now,
        )

        # Update CSR
        csr = self.csr_cache.get(request.client_id) or ClientStateRecord(
            client_id=request.client_id,
            last_transaction_id=request.transaction_id,
        )
        csr.last_transaction_id = request.transaction_id
        csr.last_activity = self.env.now
        csr.executions += 1

        # Cache response for non-idempotent operations
        if not request.idempotent:
            csr.cached_response = response
        else:
            csr.cached_response = None  # Don't waste memory on idempotent ops

        self.csr_cache.put(csr)

        # Send response
        self._send_response(response, request.client_id)

    def _send_response(self, response: Response, destination_id: int):
        """Send a response packet."""
        self._stats["responses_sent"] += 1
        self.network.send(response, destination_id)

    @property
    def stats(self) -> dict:
        """Return server statistics."""
        return {
            **self._stats,
            "csr_count": len(self.csr_cache),
        }
