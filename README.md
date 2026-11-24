# Pump.fun Paper Sprint: Any Bot Can Win

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/Falken21/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/Falken21/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A momentum paper-trading bot for the [Pump.fun Paper Sprint](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) competition on [make.fun](https://make.fun). The bot paper-trades pump.fun bonding curves on a 6-hour replay of real market data; the highest net PnL wins the pot.

## Strategy

A compact momentum bot, tuned for this competition's numbers (10 SOL capital,
max 5 open positions, max 16 orders per batch, 2-slot latency, 105,000
lamports tx fee per order, guardrail of at least 20 coins with a round trip
of at least 0.01 SOL, max drawdown 50%):

- **Entry.** A mature curve (at least 500 slots since its `CreateEvent`)
  whose recent 30-slot tape shows accelerating net SOL inflow: net buys of
  at least 1 SOL, at least 3 buy trades, at least 5 distinct buyers, price
  up at least 5% (and at most 150%) over the window, sitting within 5% of
  the window high, with the newer half of the window out-flowing the older
  half, and real SOL reserves between 2 and 60 SOL. The first buy is small:
  0.05 SOL.
- **Pyramid.** Adds 0.2 SOL per batch into strength only — price above the
  last buy's price, position in profit, net inflow still positive — up to a
  2 SOL total stake per coin.
- **Exit.** Sells the whole position on the first of: stop-loss −20% vs
  cost, trailing stop −30% from the peak value (armed once up 5%), fading
  net flow, or a 1500-slot max hold. A completed curve is closed by the
  replay itself.
- **Guardrail probes.** When the bot's own fills or coins lag the pace
  (25 coins / 240 fills over the run), it opens tiny 0.01 SOL probe round
  trips on liquid curves, so the 20-coin guardrail is met even in a quiet
  window. A 0.01 SOL buy is exactly the round-trip minimum
  (10,000,000 lamports).

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
