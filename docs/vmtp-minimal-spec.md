# VMTP Minimal Transaction Spec

Extracted from RFC 1045 for simulator design. Stripped of: multicast, security, streaming, forwarding, real-time priority, packet groups > 1 packet. This is the bone structure.

## Core Concepts

### Entity
A 64-bit identifier for a communication endpoint. Host-address independent.

```
Bits 0-3: Type flags (RAE, GRP, LEE/UGP, RES)
Bits 4-63: Domain-specific (e.g., for Domain 1: discriminant + host IP + local ID)
```

Properties required:
- **Uniqueness**: No two entities share an identifier at the same time
- **T-stability**: Cannot change meaning without being invalid for time T
- **Host independence**: Entity can migrate, be multi-homed, etc.

### Transaction
The atomic unit of communication. Identified by (Client, Transaction) tuple.

- Client: 64-bit entity identifier
- Transaction: 32-bit monotonically increasing counter

Each new request from a client increments the transaction ID.

### Message Transaction Flow

```
Client                          Server
   |                               |
   |-------- Request ------------>|
   |                               |  (process)
   |<------- Response ------------|
   |                               |
```

That's it. Two packets for a complete RPC. No connection setup. No teardown.

## Packet Formats

### Common Header (first 20 bytes)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Client (8 bytes)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|Ver|    Domain (13 bits)       |Flags|     Length (13 bits)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Control Flags (9) | RetransCount | FwdCnt | IPG |Pri|RES|F/C|
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Transaction (32 bits)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    PacketDelivery (32 bits)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Key fields for minimal implementation:
- **Client**: Who sent this (entity ID)
- **Domain**: Which entity domain (we can use 1 for Internet)
- **Length**: Size of segment data in 32-bit words
- **Transaction**: Which transaction this belongs to
- **Function Code (F/C)**: 0 = Request, 1 = Response

### Request Packet (minimal)

After common header:
```
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Server (8 bytes)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Code Flags   |         RequestCode (24 bits)                 |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    UserData (12-20 bytes)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      SegmentSize (if SDA)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Segment Data (0-16KB)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Checksum (32 bits)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Response Packet (minimal)

Same structure but:
- Function code = 1
- ResponseCode instead of RequestCode
- Server field is who responded (may differ from who was asked if group)

## Client State Machine

```
                    Send Request
                         |
                         V
                 +---------------+
        +------->| AwaitingResp  |<-------+
        |        +-------+-------+        |
        |                |                |
   Timeout          Response         Timeout
   (retry)          received         (give up)
        |                |                |
        +--------+       V       +--------+
                 | +-----------+ |
                 | | Processing| |
                 | +-----------+ |
                 +---------------+
```

States:
- **AwaitingResponse**: Sent request, waiting for response
- **Processing**: Got response (or timed out), doing local work

### Client Timeouts

- **TC1(Server)**: Expected time for response (RTT + processing estimate)
- **TC2(Server)**: Just RTT (used when retransmitting with APG flag)

On timeout: retransmit request (up to RequestRetries times, suggested: 5)

## Server State Machine

```
                  Request arrives
                         |
                         V
                 +---------------+
                 |AwaitingRequest|
                 +-------+-------+
                         |
                    Process
                         |
                         V
                 +---------------+
        +------->|  Responded    |<-------+
        |        +-------+-------+        |
        |                |                |
   Retransmit       Ack/Timeout      Timeout
   Request           received        (discard)
        |                |                |
        +--------+       V       +--------+
                 | +-----------+ |
                 | |RespDiscard| |---> AwaitingRequest
                 | +-----------+ |
                 +---------------+
```

States:
- **AwaitingRequest**: Ready to receive
- **Processing**: Executing the requested operation
- **Responded**: Sent response, holding it for potential retransmit
- **ResponseDiscarded**: Response gone, only filtering duplicates

### Server State (CSR - Client State Record)

Per-client state maintained by server:
- RemoteClient: Entity ID of client
- RemoteTransaction: Current transaction ID from this client
- Response copy (if not idempotent)

**Key insight**: Server CAN discard CSR at any time without correctness failure. It just means the next request triggers a Probe to get client state.

### Server Timeouts

- **TS4**: Time to keep CSR after responding (~500ms suggested)
- **TS5**: Time to wait for ack before retransmitting response

## Duplicate Suppression

Server uses (Client, Transaction) to detect duplicates:
1. If Transaction == RemoteTransaction in CSR: duplicate, resend Response
2. If Transaction < RemoteTransaction: old duplicate, discard
3. If Transaction > RemoteTransaction: new request, process it

## Checksum

32-bit checksum at end of packet. Algorithm: two 16-bit ones-complement sums over alternating 16-word clusters. Zero checksum = no checksum computed.

For simulator: can simplify to CRC32 or even skip if simulating in-memory.

## Minimal Implementation Notes

RFC Section 2.16 explicitly supports minimal implementations:

> "A minimal VMTP client need only support non-secure, single-packet request-response message transactions with single entity servers."

For a simulator exploring transaction semantics, we need:
1. Entity ID generation/management
2. Transaction ID tracking
3. Request/Response packet format (single packet each)
4. Client state: AwaitingResponse with timeout/retry
5. Server state: CSR with duplicate detection
6. Simulated network with configurable loss/delay/reordering

## Open Questions for Simulator Design

1. **Network model**: Do we simulate actual IP, or abstract to "deliver with probability P after delay D"?

2. **Entity mapping**: How do we resolve entity ID to "host address" in simulation? The protocol assumes some mapping exists.

3. **CSR eviction**: When server runs low on CSRs, it evicts. What policy? The protocol says any eviction is correct, just costs a Probe on next request.

4. **Failure injection**: What failure modes matter most?
   - Packet loss (random, burst, asymmetric)
   - Packet reordering
   - Node crash mid-transaction
   - Network partition

5. **Comparison baseline**: What do we compare against?
   - Simulated TCP + RPC layer?
   - Simulated QUIC + gRPC?
   - Abstract "connection-oriented transaction"?

6. **Metrics**: What do we measure?
   - Packets per transaction (happy path, failure recovery)
   - Time to detect failure
   - Correctness under partial failure
   - State held per client

---

*This is a working document for discussion. Tony: where does this diverge from how the protocol actually behaved when you built it?*
