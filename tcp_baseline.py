"""TCP-style baseline for comparison with VMTP.

Models connection-oriented transactions:
- Connection setup (3-way handshake)
- Transaction over established connection
- Connection teardown or timeout

This is NOT a full TCP implementation - it's a simplified model
that captures the connection overhead for RPC-style transactions.
"""

import simpy
from dataclasses import dataclass, field
from typing import Optional, Generator, Any
from enum import Enum, auto

from entity import Entity
from network import Network
from metrics import MetricsCollector


class TcpState(Enum):
    """TCP connection states (simplified)."""
    CLOSED = auto()
    SYN_SENT = auto()
    ESTABLISHED = auto()
    FIN_WAIT = auto()


@dataclass
class SynPacket:
    """TCP SYN packet."""
    source_id: int
    dest_id: int
    seq: int
    timestamp: float = 0.0

    def __repr__(self):
        return f"SYN(seq={self.seq})"


@dataclass
class SynAckPacket:
    """TCP SYN-ACK packet."""
    source_id: int
    dest_id: int
    seq: int
    ack: int
    timestamp: float = 0.0

    def __repr__(self):
        return f"SYN-ACK(seq={self.seq}, ack={self.ack})"


@dataclass
class AckPacket:
    """TCP ACK packet."""
    source_id: int
    dest_id: int
    ack: int
    timestamp: float = 0.0

    def __repr__(self):
        return f"ACK(ack={self.ack})"


@dataclass
class TcpRequest:
    """Request over established TCP connection."""
    source_id: int
    dest_id: int
    seq: int
    payload: Any = None
    idempotent: bool = True
    timestamp: float = 0.0

    def __repr__(self):
        return f"TcpReq(seq={self.seq})"


@dataclass
class TcpResponse:
    """Response over established TCP connection."""
    source_id: int
    dest_id: int
    seq: int
    ack: int
    payload: Any = None
    timestamp: float = 0.0

    def __repr__(self):
        return f"TcpResp(seq={self.seq}, ack={self.ack})"


@dataclass
class FinPacket:
    """TCP FIN packet."""
    source_id: int
    dest_id: int
    seq: int
    timestamp: float = 0.0

    def __repr__(self):
        return f"FIN(seq={self.seq})"


# Type alias
TcpPacket = SynPacket | SynAckPacket | AckPacket | TcpRequest | TcpResponse | FinPacket


class TcpServer:
    """TCP-style server for baseline comparison.

    Accepts connections, processes requests, sends responses.
    Maintains connection state per client.
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        metrics: MetricsCollector,
        process_time: float = 0.005,
        connection_timeout: float = 2.0,  # Drop idle connections after 2s
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.metrics = metrics
        self.process_time = process_time
        self.connection_timeout = connection_timeout

        # Connection state per client: client_id -> (state, seq, ack)
        self._connections: dict[int, tuple[TcpState, int, int]] = {}
        # Track last activity per connection for timeout
        self._last_activity: dict[int, float] = {}

        # Track executions for duplicate detection
        # Note: TCP doesn't provide this at transport layer - this simulates
        # application-level idempotency handling that you'd need to add
        self._executions: dict[int, set[int]] = {}  # client_id -> set of seqs executed

        self._stats = {
            "connections_accepted": 0,
            "connections_timed_out": 0,
            "requests_processed": 0,
            "packets_sent": 0,
            "duplicates_received": 0,  # Duplicates we had to handle
            "duplicate_executions": 0,  # Non-idempotent ops that would re-execute without tracking
        }

        self.mailbox = network.register(entity.id)
        self.env.process(self._run())
        self.env.process(self._connection_reaper())

    def _run(self):
        """Main server loop."""
        while True:
            packet = yield self.mailbox.get()
            yield self.env.process(self._handle_packet(packet))

    def _connection_reaper(self):
        """Periodically check for and close idle connections."""
        while True:
            yield self.env.timeout(0.5)  # Check every 500ms
            now = self.env.now
            to_remove = []
            for client_id, last_time in self._last_activity.items():
                if now - last_time > self.connection_timeout:
                    to_remove.append(client_id)
            for client_id in to_remove:
                self._connections.pop(client_id, None)
                self._last_activity.pop(client_id, None)
                self._executions.pop(client_id, None)
                self._stats["connections_timed_out"] += 1

    def _handle_packet(self, packet: TcpPacket):
        """Handle incoming packet based on connection state."""
        if isinstance(packet, SynPacket):
            yield self.env.process(self._handle_syn(packet))
        elif isinstance(packet, AckPacket):
            self._handle_ack(packet)
        elif isinstance(packet, TcpRequest):
            yield self.env.process(self._handle_request(packet))
        elif isinstance(packet, FinPacket):
            self._handle_fin(packet)

    def _handle_syn(self, syn: SynPacket):
        """Handle connection request."""
        client_id = syn.source_id
        server_seq = 1000  # Server's initial sequence number

        # Send SYN-ACK
        syn_ack = SynAckPacket(
            source_id=self.entity.id,
            dest_id=client_id,
            seq=server_seq,
            ack=syn.seq + 1,
            timestamp=self.env.now,
        )
        self._stats["packets_sent"] += 1
        self.network.send(syn_ack, client_id)

        # Move to state waiting for ACK (simplified: go straight to established)
        self._connections[client_id] = (TcpState.ESTABLISHED, server_seq + 1, syn.seq + 1)
        self._last_activity[client_id] = self.env.now
        self._stats["connections_accepted"] += 1
        yield self.env.timeout(0)  # Yield to keep it a generator

    def _handle_ack(self, ack: AckPacket):
        """Handle ACK packet."""
        # In simplified model, connection already established after SYN-ACK
        pass

    def _handle_request(self, request: TcpRequest):
        """Handle request over established connection."""
        client_id = request.source_id

        if client_id not in self._connections:
            # No connection - drop (in real TCP would send RST)
            # This happens after server times out the connection
            return

        # Update activity timestamp
        self._last_activity[client_id] = self.env.now

        # Check for duplicate
        if client_id not in self._executions:
            self._executions[client_id] = set()

        if request.seq in self._executions[client_id]:
            # Duplicate request received
            self._stats["duplicates_received"] += 1
            # In real TCP without app-level dedup, non-idempotent would re-execute.
            # We're simulating having that protection, but tracking when it fires.
            if not request.idempotent:
                self._stats["duplicate_executions"] += 1
            # Response still sent (TCP ACKs the data)
        else:
            # New request - execute
            self._executions[client_id].add(request.seq)
            yield self.env.timeout(self.process_time)
            self._stats["requests_processed"] += 1

        # Send response
        state, server_seq, _ = self._connections[client_id]
        response = TcpResponse(
            source_id=self.entity.id,
            dest_id=client_id,
            seq=server_seq,
            ack=request.seq + 1,
            payload=f"result-{request.seq}",
            timestamp=self.env.now,
        )
        self._connections[client_id] = (state, server_seq + 1, request.seq + 1)
        self._stats["packets_sent"] += 1
        self.network.send(response, client_id)

    def _handle_fin(self, fin: FinPacket):
        """Handle connection close."""
        client_id = fin.source_id
        if client_id in self._connections:
            # Send FIN-ACK (simplified)
            ack = AckPacket(
                source_id=self.entity.id,
                dest_id=client_id,
                ack=fin.seq + 1,
                timestamp=self.env.now,
            )
            self._stats["packets_sent"] += 1
            self.network.send(ack, client_id)
            del self._connections[client_id]

    @property
    def stats(self) -> dict:
        return self._stats.copy()


class TcpClient:
    """TCP-style client for baseline comparison.

    Establishes connections before sending requests.
    Can reuse connections for multiple transactions.
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

        self._seq = 1
        self._connections: dict[int, tuple[TcpState, int]] = {}  # server_id -> (state, server_seq)

        # Dispatcher pattern like VMTP client
        self._pending: dict[int, simpy.Event] = {}  # seq -> event
        self._responses: dict[int, TcpPacket] = {}  # seq -> response packet

        self._stats = {
            "connections_initiated": 0,
            "connections_established": 0,
            "transactions_started": 0,
            "transactions_completed": 0,
            "transactions_failed": 0,
            "packets_sent": 0,
            "retransmits": 0,
        }

        self.mailbox = network.register(entity.id)
        self.env.process(self._receiver())

    def _receiver(self):
        """Background receiver that dispatches responses."""
        while True:
            packet = yield self.mailbox.get()

            # Determine which pending request this satisfies
            if isinstance(packet, SynAckPacket):
                # Connection response - keyed by a special marker
                key = ("syn", packet.source_id)
                if key in self._pending:
                    self._responses[key] = packet
                    self._pending[key].succeed()
            elif isinstance(packet, TcpResponse):
                # Request response - keyed by ack (which is our seq + 1)
                key = packet.ack - 1
                if key in self._pending:
                    self._responses[key] = packet
                    self._pending[key].succeed()
            elif isinstance(packet, AckPacket):
                # Could be FIN-ACK or regular ACK
                pass

    def connect(self, server_id: int) -> Generator[simpy.Event, Any, bool]:
        """Establish connection to server (3-way handshake)."""
        self._stats["connections_initiated"] += 1

        syn_seq = self._seq
        self._seq += 1

        # Send SYN
        syn = SynPacket(
            source_id=self.entity.id,
            dest_id=server_id,
            seq=syn_seq,
            timestamp=self.env.now,
        )

        for attempt in range(self.default_max_retries + 1):
            self._stats["packets_sent"] += 1
            if attempt > 0:
                self._stats["retransmits"] += 1
            self.network.send(syn, server_id)

            # Wait for SYN-ACK
            key = ("syn", server_id)
            wait_event = self.env.event()
            self._pending[key] = wait_event

            timeout_event = self.env.timeout(self.default_timeout)
            result = yield wait_event | timeout_event

            self._pending.pop(key, None)

            if wait_event in result:
                syn_ack = self._responses.pop(key, None)
                if syn_ack:
                    # Send ACK to complete handshake
                    ack = AckPacket(
                        source_id=self.entity.id,
                        dest_id=server_id,
                        ack=syn_ack.seq + 1,
                        timestamp=self.env.now,
                    )
                    self._stats["packets_sent"] += 1
                    self.network.send(ack, server_id)

                    self._connections[server_id] = (TcpState.ESTABLISHED, syn_ack.seq + 1)
                    self._stats["connections_established"] += 1
                    return True

        return False  # Connection failed

    def transact(
        self,
        server_id: int,
        payload: Any = None,
        idempotent: bool = True,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> Generator[simpy.Event, Any, Optional[TcpResponse]]:
        """Execute a transaction, establishing connection if needed."""
        timeout = timeout or self.default_timeout
        max_retries = max_retries if max_retries is not None else self.default_max_retries

        self._stats["transactions_started"] += 1

        # Ensure connection exists
        if server_id not in self._connections:
            connected = yield from self.connect(server_id)
            if not connected:
                self._stats["transactions_failed"] += 1
                return None

        # Send request
        req_seq = self._seq
        self._seq += 1

        request = TcpRequest(
            source_id=self.entity.id,
            dest_id=server_id,
            seq=req_seq,
            payload=payload,
            idempotent=idempotent,
            timestamp=self.env.now,
        )

        for attempt in range(max_retries + 1):
            self._stats["packets_sent"] += 1
            if attempt > 0:
                self._stats["retransmits"] += 1
            self.network.send(request, server_id)

            # Wait for response
            wait_event = self.env.event()
            self._pending[req_seq] = wait_event

            timeout_event = self.env.timeout(timeout)
            result = yield wait_event | timeout_event

            self._pending.pop(req_seq, None)

            if wait_event in result:
                response = self._responses.pop(req_seq, None)
                if response:
                    self._stats["transactions_completed"] += 1
                    return response

        # Failed after retries
        self._stats["transactions_failed"] += 1
        # Connection may be dead - remove it so next transaction reconnects
        self._connections.pop(server_id, None)
        return None

    @property
    def stats(self) -> dict:
        return self._stats.copy()
