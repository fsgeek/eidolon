# Pre-registration: is the post-kill freeze the transport's blocking send?

**Written 2026-10-14 ~08:00 PDT during the registered campaign (run 2 of 9 in
progress), before the diagnostic image is built or run.** Outcome knowledge at
writing: run 1 only (percent_mr = 0): live regions' local commits reached 0
within ~2 s of the kill and the processes exited SIGSEGV ~6 s after it
(deviation log in `2026-10-13-caerus-artifact-preregistration.md`).

## Hypothesis (from code, `machine/connection.cc`)

Each process has one multiplexer thread doing all remote receives and sends.
Sends are blocking ZMQ PUSH `send()` (no `ZMQ_DONTWAIT`). The partial sequencer
sends a batch to every region every 5 ms epoch (`partial_sequencer.cc:220`),
dead ones included. When the queue to a dead peer reaches the send high-water
mark (ZMQ default 1000), `send()` blocks, the multiplexer stops, and the whole
process stops, regardless of workload. The SIGSEGV cause is not hypothesised.

## Manipulation

Image `caerus-run:aaf0eb6-hwm` (`external/caerus/run/Dockerfile.hwm`): the nopin
image plus `ZMQ_SNDHWM` set on each outbound PUSH socket from env
`CAERUS_SNDHWM` (unset: ZMQ default; 0: unlimited). Harness:
`external/caerus/run/run_diag.sh` (passes the env only when set). Everything
else as the registered campaign (MovR, batch 10, 240 hot records, kill
regions 2 and 5 at warm-up + 120 s, observe 600 s). One run per condition.

Metric T0: seconds from t_fail to the first per-second `Completed 0 txns/sec`
line at each live region (median over the four).

## Predictions

- **D0.** Registered campaign, percent_mr = 0 runs (default HWM): T0 < 15 s in
  every run.
- **D1.** SNDHWM = 10000, percent_mr = 0: T0 >= 5 x the median default-HWM T0
  from D0, or no freeze within 600 s.
- **D2.** SNDHWM = 0 (unlimited), percent_mr = 0: no freeze. Every 30 s window
  > 0.8 at every live region (the registered A1 statistic), and no live region
  exits during the 600 s.
- **D3.** SNDHWM = 0, percent_mr = 10 and 30: with the transport block removed,
  the originally registered mechanism should show. Report registered A2/A3/A4
  verdicts with the registered statistic.

## Falsifiers

- D2 fails (freeze within 600 s at percent_mr = 0 with unlimited HWM): the
  transport hypothesis is wrong or incomplete.
- D1 T0 within 2x of default: HWM is not what sets the freeze time.
