# VMTPsim

Note: this README file is out-of-date. (2026/03/10)

A discrete-event simulator for exploring transaction-oriented transport.

## The Question

Does transaction-as-primitive behave meaningfully differently from transaction-on-connection when things go wrong? If yes, build the real thing. If no, stop.

## Background

Modern datacenter RPC layers transactions on streaming transports (TCP, QUIC). When failures occur, the impedance mismatch manifests as ambiguous states and tail latency pathology.

VMTP (RFC 1045, 1988) offered transactions as the native primitive. A request-response is atomic. No connection to half-open, no stream to reset. The transport completes the transaction or it doesn't.

## What We Simulate

- **VMTP model**: Request-response as atomic unit, CSR-based duplicate suppression
- **Connection model**: Transaction layered on streams (simplified gRPC-like)
- **Failure injection**: Packet loss, node crash, partition
- **CWlog quorums**: O(log n) consensus over multicast vs majority quorum baseline

## What We Measure

- Packets per transaction (happy path and recovery)
- Time to failure detection
- State footprint per transaction
- Failure propagation in call chains (A → B → C)

## Status

Early exploration. Building the simulator to validate hypotheses before writing real protocol code.

## References

- RFC 1045: VMTP Specification (docs/rfc1045.txt)
- Peleg & Wool 1995: Crumbling Walls quorum systems (docs/papers/)
- Minimal spec extraction: docs/vmtp-minimal-spec.md
