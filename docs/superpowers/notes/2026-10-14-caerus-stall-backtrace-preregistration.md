# Pre-registration: where is the multiplexer thread after a peer dies?

**Written 2026-10-14 ~12:45 PDT, before the diagnostic image is built or run.**
Known at writing: rt1200 campaign (all live processes alive for 600 s after
the kill) shows, in every run including percent_mr = 0, committed and merger
throughput at 0 within ~4-10 s of the kill and for the rest of the run, with
the conflict graph empty ("remaining size 0") and scheduler pending 0: no new
transactions reach the merger at all. This is post-hoc; the claim tested here
was formed after seeing those results.

Hypothesis: each process's single `ConnectionMultiplexer::Run` thread is
blocked in a ZMQ PUSH `send()` to a dead peer (queue at the high-water mark),
so no message, local or remote, is delivered.

Method: image `caerus-run:aaf0eb6-nopin-rt1200-gdb` (rt1200 + gdb). One
six-region run, percent_mr = 0, kill regions 2 and 5 after 120 s, then at
kill + 60 s attach gdb (`--cap-add SYS_PTRACE`) to the server in each live
region and record `thread apply all bt`.

Prediction: at every live region, the thread whose stack contains
`ConnectionMultiplexer::Run` is inside a ZMQ send path
(`zmq::socket_base_t::send` / `zmq_msg_send` / `socket_t::send`).

Falsifier: at any live region the multiplexer thread is not inside a send
path (e.g., in its `recv(..., ZMQ_NOBLOCK)` or queue polling). A single
backtrace is a sample; to reduce the chance of catching the loop mid-iteration,
three backtraces are taken 2 s apart and the prediction requires all three.
