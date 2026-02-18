"""Paxos consensus on VMTP transactions.

Each Paxos phase maps to a VMTP transaction:
  - Prepare/Promise = VMTP Request/Response (Phase 1)
  - Accept/Accepted = VMTP Request/Response (Phase 2)

This is the natural fit: Paxos rounds ARE request-response transactions.
VMTP was designed for exactly this pattern.

Quorum system is pluggable - starts with majority, later steps
introduce set quorums and Flexible Paxos.
"""

import simpy
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum, auto

from entity import Entity, EntityRegistry
from packet import Request, Response
from network import Network
from metrics import MetricsCollector


# --- Paxos Messages (carried as VMTP payloads) ---

class PaxosPhase(Enum):
    PREPARE = auto()
    PROMISE = auto()
    ACCEPT = auto()
    ACCEPTED = auto()
    NACK = auto()  # Rejection


@dataclass
class Prepare:
    """Phase 1a: Proposer asks acceptors to promise."""
    proposal_number: int
    slot: int = 0  # For multi-decree Paxos


@dataclass
class Promise:
    """Phase 1b: Acceptor promises not to accept lower proposals."""
    proposal_number: int
    slot: int = 0
    # Previously accepted value (if any)
    accepted_proposal: Optional[int] = None
    accepted_value: Any = None


@dataclass
class Accept:
    """Phase 2a: Proposer asks acceptors to accept a value."""
    proposal_number: int
    slot: int = 0
    value: Any = None


@dataclass
class Accepted:
    """Phase 2b: Acceptor confirms acceptance."""
    proposal_number: int
    slot: int = 0
    value: Any = None


@dataclass
class Nack:
    """Rejection: acceptor has seen a higher proposal."""
    proposal_number: int  # The higher proposal number we've seen
    slot: int = 0


@dataclass
class PaxosPayload:
    """Wrapper that identifies a payload as a Paxos message."""
    phase: PaxosPhase
    message: Prepare | Promise | Accept | Accepted | Nack


# --- Quorum Systems ---

class QuorumSystem:
    """Base class for quorum systems. Subclass for different constructions."""

    def __init__(self, nodes: list[int]):
        self.nodes = list(nodes)
        self.n = len(nodes)

    def phase1_quorum_size(self) -> int:
        """How many responses needed for Phase 1 (Prepare/Promise)."""
        raise NotImplementedError

    def phase2_quorum_size(self) -> int:
        """How many responses needed for Phase 2 (Accept/Accepted)."""
        raise NotImplementedError

    def is_phase1_quorum(self, respondents: set[int]) -> bool:
        """Check if we have enough Phase 1 responses."""
        return len(respondents & set(self.nodes)) >= self.phase1_quorum_size()

    def is_phase2_quorum(self, respondents: set[int]) -> bool:
        """Check if we have enough Phase 2 responses."""
        return len(respondents & set(self.nodes)) >= self.phase2_quorum_size()


class MajorityQuorum(QuorumSystem):
    """Classic majority quorum: floor(n/2) + 1."""

    def phase1_quorum_size(self) -> int:
        return self.n // 2 + 1

    def phase2_quorum_size(self) -> int:
        return self.n // 2 + 1


class FlexibleQuorum(QuorumSystem):
    """Flexible Paxos (Howard et al.): Phase 1 and Phase 2 quorums
    only need to intersect, not each be a majority.

    Requirement: phase1_size + phase2_size > n
    """

    def __init__(self, nodes: list[int], phase1_size: int, phase2_size: int):
        super().__init__(nodes)
        self._phase1_size = phase1_size
        self._phase2_size = phase2_size
        if phase1_size + phase2_size <= self.n:
            raise ValueError(
                f"Flexible Paxos requires q1 + q2 > n: "
                f"{phase1_size} + {phase2_size} <= {self.n}"
            )

    def phase1_quorum_size(self) -> int:
        return self._phase1_size

    def phase2_quorum_size(self) -> int:
        return self._phase2_size


# --- Acceptor ---

class Acceptor:
    """Paxos Acceptor running as a VMTP server.

    Receives Prepare/Accept requests as VMTP transactions.
    Responds with Promise/Accepted/Nack as VMTP responses.
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        process_time: float = 0.001,  # 1ms to process
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.process_time = process_time

        # Paxos state per slot
        self._highest_promised: dict[int, int] = {}  # slot -> highest proposal promised
        self._accepted: dict[int, tuple[int, Any]] = {}  # slot -> (proposal, value)

        self._stats = {
            "prepares_received": 0,
            "promises_sent": 0,
            "accepts_received": 0,
            "accepteds_sent": 0,
            "nacks_sent": 0,
        }

        # Register and start
        self.mailbox = network.register(entity.id)
        self.env.process(self._run())

    def _run(self):
        """Main loop: receive VMTP requests, process Paxos messages."""
        while True:
            packet = yield self.mailbox.get()
            if not isinstance(packet, Request):
                continue
            if not isinstance(packet.payload, PaxosPayload):
                continue

            yield self.env.process(self._handle(packet))

    def _handle(self, request: Request):
        """Handle a Paxos request."""
        payload = request.payload

        if payload.phase == PaxosPhase.PREPARE:
            yield self.env.process(self._handle_prepare(request, payload.message))
        elif payload.phase == PaxosPhase.ACCEPT:
            yield self.env.process(self._handle_accept(request, payload.message))

    def _handle_prepare(self, request: Request, prepare: Prepare):
        """Phase 1b: Respond to Prepare with Promise or Nack."""
        self._stats["prepares_received"] += 1
        yield self.env.timeout(self.process_time)

        slot = prepare.slot
        highest = self._highest_promised.get(slot, -1)

        if prepare.proposal_number > highest:
            # Promise: we won't accept anything lower
            self._highest_promised[slot] = prepare.proposal_number

            # Include any previously accepted value
            accepted = self._accepted.get(slot)
            if accepted:
                promise = Promise(
                    proposal_number=prepare.proposal_number,
                    slot=slot,
                    accepted_proposal=accepted[0],
                    accepted_value=accepted[1],
                )
            else:
                promise = Promise(
                    proposal_number=prepare.proposal_number,
                    slot=slot,
                )

            self._stats["promises_sent"] += 1
            self._send_response(request, PaxosPayload(PaxosPhase.PROMISE, promise))
        else:
            # Nack: we've already promised higher
            nack = Nack(proposal_number=highest, slot=slot)
            self._stats["nacks_sent"] += 1
            self._send_response(request, PaxosPayload(PaxosPhase.NACK, nack))

    def _handle_accept(self, request: Request, accept: Accept):
        """Phase 2b: Respond to Accept with Accepted or Nack."""
        self._stats["accepts_received"] += 1
        yield self.env.timeout(self.process_time)

        slot = accept.slot
        highest = self._highest_promised.get(slot, -1)

        if accept.proposal_number >= highest:
            # Accept the value
            self._highest_promised[slot] = accept.proposal_number
            self._accepted[slot] = (accept.proposal_number, accept.value)

            accepted = Accepted(
                proposal_number=accept.proposal_number,
                slot=slot,
                value=accept.value,
            )
            self._stats["accepteds_sent"] += 1
            self._send_response(request, PaxosPayload(PaxosPhase.ACCEPTED, accepted))
        else:
            # Nack
            nack = Nack(proposal_number=highest, slot=slot)
            self._stats["nacks_sent"] += 1
            self._send_response(request, PaxosPayload(PaxosPhase.NACK, nack))

    def _send_response(self, request: Request, payload: PaxosPayload):
        """Send a VMTP response carrying a Paxos message."""
        response = Response(
            client_id=request.client_id,
            server_id=self.entity.id,
            transaction_id=request.transaction_id,
            payload=payload,
            timestamp=self.env.now,
        )
        self.network.send(response, request.client_id)

    @property
    def stats(self) -> dict:
        return self._stats.copy()


# --- Proposer ---

@dataclass
class ConsensusResult:
    """Result of a Paxos consensus attempt."""
    success: bool
    slot: int
    value: Any = None
    proposal_number: int = 0
    phase1_responses: int = 0
    phase2_responses: int = 0
    nacks: int = 0
    rounds: int = 0  # How many proposal rounds needed
    total_time: float = 0.0
    packets_sent: int = 0


class Proposer:
    """Paxos Proposer using VMTP transactions.

    Sends Prepare to acceptors, collects Promises.
    Sends Accept to acceptors, collects Accepted.

    Each fan-out to acceptors uses individual VMTP transactions.
    (Future: VMTP multicast could send one packet to all.)
    """

    def __init__(
        self,
        env: simpy.Environment,
        entity: Entity,
        network: Network,
        acceptor_ids: list[int],
        quorum: QuorumSystem,
        timeout: float = 0.500,  # 500ms default timeout per phase
        max_rounds: int = 10,
    ):
        self.env = env
        self.entity = entity
        self.network = network
        self.acceptor_ids = acceptor_ids
        self.quorum = quorum
        self.timeout = timeout
        self.max_rounds = max_rounds

        # Proposal number: (round * 1000) + proposer_id for uniqueness
        self._proposal_counter = 0

        # Mailbox for receiving responses
        self.mailbox = network.register(entity.id)

        # Pending responses: transaction_id -> Event
        self._pending: dict[int, simpy.Event] = {}
        self._responses: dict[int, Response] = {}
        self._next_txn_id = 1

        self._stats = {
            "proposals_started": 0,
            "proposals_succeeded": 0,
            "proposals_failed": 0,
            "phase1_rounds": 0,
            "phase2_rounds": 0,
            "total_packets_sent": 0,
        }

        # Start receiver
        self.env.process(self._receiver())

    def _receiver(self):
        """Background process receiving VMTP responses."""
        while True:
            packet = yield self.mailbox.get()
            if isinstance(packet, Response):
                txn_id = packet.transaction_id
                if txn_id in self._pending:
                    self._responses[txn_id] = packet
                    self._pending[txn_id].succeed()

    def _next_proposal_number(self) -> int:
        self._proposal_counter += 1
        return self._proposal_counter * 1000 + self.entity.id

    def _send_to_acceptor(self, acceptor_id: int, payload: PaxosPayload) -> int:
        """Send a VMTP request to an acceptor. Returns transaction ID."""
        txn_id = self._next_txn_id
        self._next_txn_id += 1

        request = Request(
            client_id=self.entity.id,
            server_id=acceptor_id,
            transaction_id=txn_id,
            idempotent=True,  # Paxos messages are idempotent by design
            payload=payload,
            timestamp=self.env.now,
        )

        self._stats["total_packets_sent"] += 1
        self.network.send(request, acceptor_id)
        return txn_id

    def _collect_responses(self, txn_ids: list[int], needed: int,
                           timeout: float) -> list[Response]:
        """Wait for enough responses or timeout. Returns collected responses."""
        # Create events for each pending transaction
        for txn_id in txn_ids:
            self._pending[txn_id] = self.env.event()

        responses = []
        deadline = self.env.now + timeout

        while len(responses) < needed and self.env.now < deadline:
            # Wait for any pending event or timeout
            remaining = deadline - self.env.now
            if remaining <= 0:
                break

            # Build list of pending events
            pending_events = [
                self._pending[tid] for tid in txn_ids
                if tid in self._pending
            ]
            if not pending_events:
                break

            timeout_event = self.env.timeout(remaining)
            result = yield simpy.events.AnyOf(self.env, pending_events + [timeout_event])

            # Collect any responses that arrived
            for txn_id in list(txn_ids):
                if txn_id in self._responses:
                    responses.append(self._responses.pop(txn_id))
                    self._pending.pop(txn_id, None)

        # Cleanup remaining pending
        for txn_id in txn_ids:
            self._pending.pop(txn_id, None)
            self._responses.pop(txn_id, None)

        return responses

    def propose(self, slot: int, value: Any) -> simpy.Process:
        """Run Paxos to achieve consensus on a value for a slot.

        Returns a SimPy process. Yield it to get a ConsensusResult.
        """
        return self.env.process(self._propose(slot, value))

    def _propose(self, slot: int, value: Any):
        """Internal: run Paxos rounds until consensus or max_rounds."""
        self._stats["proposals_started"] += 1
        start_time = self.env.now
        total_phase1 = 0
        total_phase2 = 0
        total_nacks = 0
        packets = 0

        for round_num in range(self.max_rounds):
            proposal_number = self._next_proposal_number()

            # --- Phase 1: Prepare ---
            self._stats["phase1_rounds"] += 1
            prepare = Prepare(proposal_number=proposal_number, slot=slot)
            payload = PaxosPayload(PaxosPhase.PREPARE, prepare)

            txn_ids = []
            for acc_id in self.acceptor_ids:
                txn_id = self._send_to_acceptor(acc_id, payload)
                txn_ids.append(txn_id)
                packets += 1

            # Collect Phase 1 responses
            needed = self.quorum.phase1_quorum_size()
            responses = yield from self._collect_responses(txn_ids, needed, self.timeout)

            # Parse responses
            promises = []
            phase1_nacks = 0
            for resp in responses:
                if isinstance(resp.payload, PaxosPayload):
                    if resp.payload.phase == PaxosPhase.PROMISE:
                        promises.append(resp.payload.message)
                    elif resp.payload.phase == PaxosPhase.NACK:
                        phase1_nacks += 1

            total_phase1 += len(promises)
            total_nacks += phase1_nacks

            if len(promises) < needed:
                # Didn't get quorum - retry with higher proposal
                yield self.env.timeout(0.010 * (round_num + 1))  # Backoff
                continue

            # Check if any acceptor already accepted a value
            # Must use the highest-numbered accepted value
            highest_accepted = None
            for p in promises:
                if p.accepted_proposal is not None:
                    if highest_accepted is None or p.accepted_proposal > highest_accepted[0]:
                        highest_accepted = (p.accepted_proposal, p.accepted_value)

            chosen_value = highest_accepted[1] if highest_accepted else value

            # --- Phase 2: Accept ---
            self._stats["phase2_rounds"] += 1
            accept = Accept(
                proposal_number=proposal_number,
                slot=slot,
                value=chosen_value,
            )
            payload = PaxosPayload(PaxosPhase.ACCEPT, accept)

            txn_ids = []
            for acc_id in self.acceptor_ids:
                txn_id = self._send_to_acceptor(acc_id, payload)
                txn_ids.append(txn_id)
                packets += 1

            # Collect Phase 2 responses
            needed = self.quorum.phase2_quorum_size()
            responses = yield from self._collect_responses(txn_ids, needed, self.timeout)

            # Parse responses
            accepteds = []
            phase2_nacks = 0
            for resp in responses:
                if isinstance(resp.payload, PaxosPayload):
                    if resp.payload.phase == PaxosPhase.ACCEPTED:
                        accepteds.append(resp.payload.message)
                    elif resp.payload.phase == PaxosPhase.NACK:
                        phase2_nacks += 1

            total_phase2 += len(accepteds)
            total_nacks += phase2_nacks

            if len(accepteds) >= needed:
                # Consensus achieved!
                self._stats["proposals_succeeded"] += 1
                return ConsensusResult(
                    success=True,
                    slot=slot,
                    value=chosen_value,
                    proposal_number=proposal_number,
                    phase1_responses=total_phase1,
                    phase2_responses=total_phase2,
                    nacks=total_nacks,
                    rounds=round_num + 1,
                    total_time=self.env.now - start_time,
                    packets_sent=packets,
                )

            # Phase 2 failed - retry
            yield self.env.timeout(0.010 * (round_num + 1))

        # Exhausted rounds
        self._stats["proposals_failed"] += 1
        return ConsensusResult(
            success=False,
            slot=slot,
            value=None,
            proposal_number=0,
            phase1_responses=total_phase1,
            phase2_responses=total_phase2,
            nacks=total_nacks,
            rounds=self.max_rounds,
            total_time=self.env.now - start_time,
            packets_sent=packets,
        )

    @property
    def stats(self) -> dict:
        return self._stats.copy()


# --- Learner ---

class Learner:
    """Paxos Learner: observes accepted values.

    In this simulation, the proposer already knows the result.
    The learner tracks the decided log for verification.
    """

    def __init__(self):
        self._decided: dict[int, Any] = {}  # slot -> decided value

    def learn(self, slot: int, value: Any):
        """Record a decided value."""
        if slot in self._decided:
            # Already decided - should be same value (consistency check)
            assert self._decided[slot] == value, \
                f"Consistency violation! Slot {slot}: {self._decided[slot]} vs {value}"
        self._decided[slot] = value

    def get(self, slot: int) -> Optional[Any]:
        return self._decided.get(slot)

    @property
    def decided(self) -> dict[int, Any]:
        return dict(self._decided)

    def __len__(self) -> int:
        return len(self._decided)
