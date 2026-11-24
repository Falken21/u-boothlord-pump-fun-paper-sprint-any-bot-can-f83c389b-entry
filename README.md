# Pump.fun Paper Sprint: Any Bot Can Win

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/Falken21/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/Falken21/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A momentum paper-trading bot for the [Pump.fun Paper Sprint](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) competition on [make.fun](https://make.fun). The bot paper-trades pump.fun bonding curves on a 6-hour replay of real market data; the highest net PnL wins the pot.

## Strategy (v2)

Rebuilt after v1's local grade missed the guardrail entirely (3 coins
traded, about −9.7 SOL on the dev window): its entry filters were so strict
that almost nothing fired, and its few 2 SOL positions bled out. V2 is
guardrail-first, with a smaller, faster momentum sleeve. Tuned for this
competition's numbers (10 SOL capital, max 5 open positions, max 16 orders
per batch, 2-slot latency, 105,000 lamports tx fee per order, guardrail of
at least 20 coins with a round trip of at least 0.01 SOL, max drawdown 50%):

- **Guardrail probes first.** Every mint whose real SOL reserve reaches
  0.55 SOL joins a probe queue. A probe accumulates ~0.012 SOL of buy fills
  — stacked in tranches across batches when the 1%-of-reserve cap makes a
  single buy smaller than the 0.01 SOL round-trip minimum — then sells the
  whole position a few slots later, whatever the PnL. Probes have priority
  over momentum for free position slots and aim for 30 counted round trips
  (the guardrail needs 20). On the dev window the probe engine is roughly
  break-even after fees; it exists to make compliance a certainty, not a
  hope.
- **Momentum, looser and smaller.** Entry needs a curve at least 60 slots
  old with at least 10 observed trades, net inflow of at least 0.4 SOL over
  the last 60 slots (or 0.7 SOL of buy volume at a 60%+ buy share), at
  least 4 distinct buyers in the window, price up at least 3% over the
  last 120 slots but no more than 1.6× the 300-slot base low (no chasing
  parabolic tops), and real reserves of at least 1.5 SOL. First buy
  0.06 SOL, at most one pyramid add of 0.12 SOL into strength, hard stake
  cap 0.35 SOL per mint.
- **Fast exits.** Stop-loss at −12% versus average cost; trailing stop
  armed at +8% and firing 15% below the peak; half the position sold if
  the price doubles; 600-slot max hold. A drawdown circuit breaker halts
  new momentum entries if equity falls 35% below its peak (probes keep
  running). A completed curve is closed by the replay itself.
- **Fee discipline.** Pending orders are tracked against cash, a cash
  buffer is always kept, and orders are emitted in priority order — exits,
  probe sells/buys, then momentum — inside the 16-order batch cap.

Local grade on the kit's frozen 24 h dev window (process driver):
guardrail-compliant with 331 counted round trips over 181 mints, max
drawdown 17.6%, net PnL −1.36 SOL (v1: non-compliant, −9.7 SOL). The dev
window is one sample; the final ranking runs on a separate 6-hour window.

## How it works

`main.py` (Python 3.13 standard library only) speaks the
`agent-fun/replay/v2` protocol on stdin/stdout: it reads one `hello`, then
answers every `batch` with exactly one `reply` echoing the batch's `seq`,
and exits cleanly on `done`. It keeps each curve's latest reserves and a
rolling trade tape, tracks positions, pending orders and its own fills, and
decides orders under the competition caps (max 5 open positions, max 16
orders per batch, buys capped at 1% of a mint's real SOL reserve).

The bot is fully deterministic: no wall clock, no environment reads, no
unseeded randomness — two runs produce byte-identical action logs.

## Run it

```sh
PYTHONHASHSEED=0 python3 -P -s main.py
```

Feed it `hello`, `batch`, … lines of NDJSON on stdin; it writes one `reply`
line per batch on stdout (diagnostics, if any, go to stderr) and exits
after `done`.

## License

MIT — see [LICENSE](LICENSE).
