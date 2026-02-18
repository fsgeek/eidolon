"""QUIC baseline for comparison with VMTP.

QUIC is the real modern competitor, not TCP. It has:
- 0-RTT resumption for known servers (session tickets)
- 1-RTT handshake for new connections (vs TCP's 1.5 RTT + TLS)
- Connection ID allows migration without full reconnection
- Better loss recovery (per-stream, not head-of-line blocking)

This model captures the transaction-relevant semantics, not crypto.
"""

import simpy
from dataclasses import dataclass
from typing import Optional, Any, Generator
from enum import Enum, auto

from entity import Entity
from network import Network
from metrics import MetricsCollector


class QuicConnectionState(Enum):
    """QUIC connection states."""
    NONE = auto()
    HANDSHAKING = auto()
    ESTABLISHED = auto()
    RESUMABLE = auto()  # Have session ticket, can do 0-RTT


@dataclass
class QuicInitial:
    """QUIC Initial packet (starts handshake)."""
    source_id: int
    dest_id: int
    conn_id: int  # Connection ID - allows migration
    timestamp: float = 0.0


@dataclass
class QuicHandshake:
    """QUIC Handshake response."""
    source_id: int
    dest_id: int
    conn_id: int
    session_ticket: bytes = b""  # For 0-RTT resumption
    timestamp: float = 0.0


@dataclass
class QuicZeroRtt:
    """QUIC 0-RTT packet (early data with resumption)."""
    source_id: int
    dest_id: int
    conn_id: int
    payload: Any = None
    timestamp: float = 0.0


@dataclass
class QuicRequest:
    """Request over established QUIC connection."""
    source_id: int
    dest_id: int
    conn_id: int
    stream_id: int
    payload: Any = None
    idempotent: bool = True
    timestamp: float = 0.0


@dataclass
class QuicResponse:
    """Response over QUIC connection."""
    source_id: int
    dest_id: int
    conn_id: int
    stream_id: int
    payload: Any = None
    timestamp: float = 0.0


@dataclass
class QuicAck:
    """QUIC ACK packet."""
    source_id: int
    dest_id: int
    conn_id: int
    timestamp: float = 0.0


QuicPacket = QuicInitial | QuicHandshake | QuicZeroRtt | QuicRequest | QuicResponse | QuicAck


class QuicServer:
    """QUIC server implementation."""

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        metrics: MetricsCollector,
        process_time: float = 0.005,
        connection_timeout: float = 30.0,  # QUIC connections live longer
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.metrics = metrics
        self.process_time = process_time
        self.connection_timeout = connection_timeout

        # Connection state: conn_id -> state info
        self._connections: dict[int, dict] = {}
        # Session tickets for 0-RTT: client_id -> ticket
        self._session_tickets: dict[int, bytes] = {}
        # Execution tracking
        self._executions: dict[int, set[int]] = {}

        self._stats = {
            "connections_accepted": 0,
            "zero_rtt_accepted": 0,
            "requests_processed": 0,
            "packets_sent": 0,
            "duplicate_executions": 0,
        }

        self.mailbox = network.register(entity.id)
        self.env.process(self._run())

    def _run(self):
        while True:
            packet = yield self.mailbox.get()
            yield self.env.process(self._handle_packet(packet))

    def _handle_packet(self, packet: QuicPacket):
        if isinstance(packet, QuicInitial):
            yield self.env.process(self._handle_initial(packet))
        elif isinstance(packet, QuicZeroRtt):
            yield self.env.process(self._handle_zero_rtt(packet))
        elif isinstance(packet, QuicRequest):
            yield self.env.process(self._handle_request(packet))

    def _handle_initial(self, initial: QuicInitial):
        """Handle new connection."""
        # Send handshake response with session ticket
        ticket = f"ticket-{initial.source_id}-{self.env.now}".encode()
        self._session_tickets[initial.source_id] = ticket

        handshake = QuicHandshake(
            source_id=self.entity.id,
            dest_id=initial.source_id,
            conn_id=initial.conn_id,
            session_ticket=ticket,
            timestamp=self.env.now,
        )
        self._stats["packets_sent"] += 1
        self.network.send(handshake, initial.source_id)

        self._connections[initial.conn_id] = {
            "client_id": initial.source_id,
            "state": QuicConnectionState.ESTABLISHED,
            "last_activity": self.env.now,
        }
        self._stats["connections_accepted"] += 1
        yield self.env.timeout(0)

    def _handle_zero_rtt(self, packet: QuicZeroRtt):
        """Handle 0-RTT early data."""
        # Verify session ticket exists
        if packet.source_id not in self._session_tickets:
            # No ticket - reject 0-RTT, client must do full handshake
            return

        self._stats["zero_rtt_accepted"] += 1

        # Accept 0-RTT, establish connection
        self._connections[packet.conn_id] = {
            "client_id": packet.source_id,
            "state": QuicConnectionState.ESTABLISHED,
            "last_activity": self.env.now,
        }

        # Process the early data as a request
        yield self.env.timeout(self.process_time)
        self._stats["requests_processed"] += 1

        response = QuicResponse(
            source_id=self.entity.id,
            dest_id=packet.source_id,
            conn_id=packet.conn_id,
            stream_id=0,
            payload=f"0rtt-result",
            timestamp=self.env.now,
        )
        self._stats["packets_sent"] += 1
        self.network.send(response, packet.source_id)

    def _handle_request(self, request: QuicRequest):
        """Handle request on established connection."""
        if request.conn_id not in self._connections:
            return

        self._connections[request.conn_id]["last_activity"] = self.env.now

        # Process
        yield self.env.timeout(self.process_time)
        self._stats["requests_processed"] += 1

        response = QuicResponse(
            source_id=self.entity.id,
            dest_id=request.source_id,
            conn_id=request.conn_id,
            stream_id=request.stream_id,
            payload=f"result-{request.stream_id}",
            timestamp=self.env.now,
        )
        self._stats["packets_sent"] += 1
        self.network.send(response, request.source_id)

    @property
    def stats(self):
        return self._stats.copy()


class QuicClient:
    """QUIC client implementation."""

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

        self._conn_id_counter = 1
        self._stream_id_counter = 1

        # Connection state per server
        self._connections: dict[int, dict] = {}
        # Session tickets for 0-RTT
        self._session_tickets: dict[int, bytes] = {}

        # Dispatcher
        self._pending: dict[tuple, simpy.Event] = {}
        self._responses: dict[tuple, QuicPacket] = {}

        self._stats = {
            "connections_initiated": 0,
            "zero_rtt_attempts": 0,
            "transactions_started": 0,
            "transactions_completed": 0,
            "transactions_failed": 0,
            "packets_sent": 0,
        }

        self.mailbox = network.register(entity.id)
        self.env.process(self._receiver())

    def _receiver(self):
        while True:
            packet = yield self.mailbox.get()

            if isinstance(packet, QuicHandshake):
                # Save session ticket for 0-RTT
                self._session_tickets[packet.source_id] = packet.session_ticket
                key = ("handshake", packet.conn_id)
                if key in self._pending:
                    self._responses[key] = packet
                    self._pending[key].succeed()
            elif isinstance(packet, QuicResponse):
                key = ("response", packet.conn_id, packet.stream_id)
                if key in self._pending:
                    self._responses[key] = packet
                    self._pending[key].succeed()

    def connect(self, server_id: int) -> Generator[simpy.Event, Any, bool]:
        """Establish QUIC connection.

        1-RTT if new server, 0-RTT if we have session ticket.
        """
        conn_id = self._conn_id_counter
        self._conn_id_counter += 1

        # Check for 0-RTT capability
        if server_id in self._session_tickets:
            # We can try 0-RTT
            self._stats["zero_rtt_attempts"] += 1
            self._connections[server_id] = {
                "conn_id": conn_id,
                "state": QuicConnectionState.RESUMABLE,
            }
            return True

        # Full handshake (1-RTT)
        self._stats["connections_initiated"] += 1
        initial = QuicInitial(
            source_id=self.entity.id,
            dest_id=server_id,
            conn_id=conn_id,
            timestamp=self.env.now,
        )

        self._stats["packets_sent"] += 1
        self.network.send(initial, server_id)

        # Wait for handshake
        key = ("handshake", conn_id)
        wait_event = self.env.event()
        self._pending[key] = wait_event

        timeout_event = self.env.timeout(self.default_timeout)
        result = yield wait_event | timeout_event

        self._pending.pop(key, None)

        if wait_event in result:
            # Send ACK to complete handshake
            ack = QuicAck(
                source_id=self.entity.id,
                dest_id=server_id,
                conn_id=conn_id,
                timestamp=self.env.now,
            )
            self._stats["packets_sent"] += 1
            self.network.send(ack, server_id)

            self._connections[server_id] = {
                "conn_id": conn_id,
                "state": QuicConnectionState.ESTABLISHED,
            }
            return True

        return False

    def transact(
        self,
        server_id: int,
        payload: Any = None,
        idempotent: bool = True,
    ) -> Generator[simpy.Event, Any, Optional[QuicResponse]]:
        """Execute transaction over QUIC."""
        self._stats["transactions_started"] += 1

        # Ensure connection
        if server_id not in self._connections:
            connected = yield from self.connect(server_id)
            if not connected:
                self._stats["transactions_failed"] += 1
                return None

        conn_info = self._connections[server_id]
        conn_id = conn_info["conn_id"]

        stream_id = self._stream_id_counter
        self._stream_id_counter += 1

        # Check for 0-RTT
        if conn_info["state"] == QuicConnectionState.RESUMABLE:
            # Send 0-RTT with early data
            zero_rtt = QuicZeroRtt(
                source_id=self.entity.id,
                dest_id=server_id,
                conn_id=conn_id,
                payload=payload,
                timestamp=self.env.now,
            )
            self._stats["packets_sent"] += 1
            self.network.send(zero_rtt, server_id)

            # Update state
            conn_info["state"] = QuicConnectionState.ESTABLISHED
        else:
            # Normal request
            request = QuicRequest(
                source_id=self.entity.id,
                dest_id=server_id,
                conn_id=conn_id,
                stream_id=stream_id,
                payload=payload,
                idempotent=idempotent,
                timestamp=self.env.now,
            )
            self._stats["packets_sent"] += 1
            self.network.send(request, server_id)

        # Wait for response
        key = ("response", conn_id, stream_id if conn_info["state"] == QuicConnectionState.ESTABLISHED else 0)
        wait_event = self.env.event()
        self._pending[key] = wait_event

        timeout_event = self.env.timeout(self.default_timeout)
        result = yield wait_event | timeout_event

        self._pending.pop(key, None)

        if wait_event in result:
            self._stats["transactions_completed"] += 1
            return self._responses.pop(key, None)

        self._stats["transactions_failed"] += 1
        return None

    @property
    def stats(self):
        return self._stats.copy()
