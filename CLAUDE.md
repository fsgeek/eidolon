# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository is for exploring and implementing VMTP (Versatile Message Transaction Protocol) based on RFC 1045. VMTP is a transport protocol designed specifically for remote procedure call (RPC) and transaction-oriented communication.

**Current State**: Early exploration. Building a simulator to test whether VMTP's transaction-as-primitive model behaves meaningfully differently from modern transaction-on-streams (gRPC/QUIC) under failure conditions.

**Target**: Datacenter protocol. Datacenters control their fabric, can accept IP protocol 81, and can run IP multicast - making VMTP's design viable there even if the public internet has ossified around TCP/UDP.

## Development Environment

```bash
# Use uv, not pip
uv sync              # Install dependencies
uv run python ...    # Run scripts
uv run pytest        # Run tests
```

**Important**: This project uses `uv` for Python environment management. Do not use `pip` directly.

## Key Documents

- `docs/rfc1045.txt` - Complete RFC 1045 specification (the primary source)
- `docs/vmtp-minimal-spec.md` - Extracted minimal transaction protocol for simulator design

## What is VMTP?

VMTP addresses TCP's inefficiencies for RPC by providing:
- Minimal two-packet exchanges for simple transactions (vs TCP's connection setup/teardown overhead)
- Stateless transaction model - no connection state between transactions
- Host-address independent entity identifiers (solves process migration, mobile hosts)
- Built-in multicast, real-time, and security support

## Key Architectural Concepts

**Entities**: Transport-level endpoints identified by stable, 64-bit host-address independent identifiers. Entities can be processes, services, or any addressable endpoint.

**Message Transactions**: The fundamental communication unit - a request-response exchange as an atomic operation. The client sends a request, server processes it, server responds.

**Entity Domains**: Different naming and allocation schemes for entity identifiers (domain 1 for Internet, domain 3 for Stanford).

**Packet Groups**: Multiple packets transmitted together to support streaming for large data transfers.

**Client State Record (CSR)**: Server-maintained state about remote clients. Servers can discard CSRs without affecting correctness (stateless design).

## Protocol Architecture (from RFC 1045)

1. **Client Protocol Module**: Initiates transactions, handles retransmissions, manages timeouts, processes responses
2. **Server Protocol Module**: Receives requests, manages CSRs, sends responses, handles duplicate detection
3. **Management Module**: Entity creation/deletion, authentication, group membership
4. **Reliability Layer**: Checksums, acknowledgments, retransmissions, duplicate suppression

Both client and server implement explicit state machines with event-driven processing (user events, packet arrivals, timeouts).

## Key Differences from TCP

| Aspect | TCP | VMTP |
|--------|-----|------|
| Model | Stream-oriented | Transaction-oriented |
| Connection | Requires setup/teardown | Stateless transactions |
| RPC cost | Multiple round-trips | 2 packets minimum |
| Naming | IP:port (host-bound) | Entity IDs (host-independent) |
| Multicast | Not supported | Native support |

## Subsettability

VMTP is designed for minimal implementations - useful for PROM boot loaders, embedded sensors, simple controllers. Not all features need to be implemented.

## RFC 1045 Structure

Key sections for implementation:
- Section 3: Packet formats and field definitions
- Section 4: Client protocol state machine and events
- Section 5: Server protocol state machine and events
- Appendix VI: IP implementation specifics
- Appendix VII: Implementation notes and data structures
- Appendix VIII: UNIX 4.3 BSD kernel interface

## Terminology

- **Transaction ID**: 32-bit identifier for a request-response exchange
- **Run**: Sequence of packet groups (for streaming)
- **Delivery Mask**: Bitmap indicating which packets in a group were received
- **CMG (Co-resident Module Group)**: Entities sharing an address space
