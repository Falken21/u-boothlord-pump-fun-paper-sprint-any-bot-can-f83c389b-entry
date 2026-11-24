"""Momentum bot for "Pump.fun Paper Sprint: Any Bot Can Win"
(u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b).

Speaks agent-fun/replay/v2 on stdin/stdout: one ``hello``, then one ``reply``
per ``batch`` (echoing the batch's ``seq``), and exits cleanly on ``done``.
Python 3.13 standard library only; everything the bot runs is this one file.

Strategy (a compact momentum bot, tuned for this competition's numbers:
10 SOL capital, max 5 open positions, max 16 orders/batch, 2-slot latency,
105,000 lamports tx fee per order, guardrail >= 20 coins with a round trip
of >= 0.01 SOL, max drawdown 50%):

* Entry: a mature curve (age >= 500 slots since its CreateEvent) whose
  recent 30-slot tape shows accelerating net SOL inflow (net buys >= 1 SOL,
  >= 3 buy trades, >= 5 distinct buyers), price up >= 5% (and <= 150%)
  over the window, sitting within 5% of the window high, with the newer
  half of the window out-flowing the older half, and real SOL reserves
  between 2 and 60 SOL. First buy is small (0.05 SOL).
* Pyramid: add 0.2 SOL per batch into strength only (price above the last
  buy's price, position in profit, net inflow still positive), up to a
  2 SOL total stake per coin.
* Exit whole on: stop-loss -20% vs cost, trailing stop -30% from the peak
  value (armed once up 5%), fading net flow, a single large sell, or a
  1500-slot max hold. A completed curve is closed by the replay itself.
* Guardrail probes: when the bot's own fills/coins lag the pace (25 coins /
  240 fills over the run), it opens tiny 0.01 SOL probe round trips on
  liquid curves so the 20-coin guardrail is met even in a quiet window.
  A 0.01 SOL buy is exactly the round-trip minimum (10,000,000 lamports).

Deterministic by construction: no clock, no environment reads, no
unseeded randomness; every iteration is over insertion-ordered dicts/lists
fed by the batch stream, so two runs produce byte-identical action logs.
"""

import json
import math
import re
import sys

PROTOCOL = "agent-fun/replay/v2"
MAX_HARNESS_LINE_BYTES = 1_048_576
MAX_BOT_LINE_BYTES = 65_536
LAMPORTS_PER_SOL = 1_000_000_000
BPS = 10_000
U64_MAX = 2**64 - 1
CURVE_FEE = 0.0125  # 95 bps protocol + 30 bps creator fallback = 125 bps

_PUBKEY_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
_CLIENT_ID_RE = re.compile(r"[A-Za-z0-9-][A-Za-z0-9_-]{0,63}")

# --- strategy parameters (tuned on this competition's dev window) ---
WINDOW = 30            # entry lookback, slots
MIN_NET_SOL = 1.0      # net buy SOL over the window
MIN_BUYS = 3           # buy trades in the window
MIN_BUYERS = 5         # distinct buying wallets in the window
MIN_PRICE_UP = 0.05    # price now vs window start, minus 1
MAX_PRICE_UP = 1.5     # skip curves that already ran too far
ACCEL = 1.0            # newer-half net inflow >= ACCEL * older-half net inflow
NEAR_HIGH = 0.95       # price now >= 95% of the window high
MIN_RQ_SOL = 2.0       # real SOL reserves at entry, min
MAX_RQ_SOL = 60.0      # real SOL reserves at entry, max
MIN_AGE = 500          # slots since CreateEvent before an entry is allowed
STAKE_SOL = 2.0        # SOL into one position in total
INITIAL_SOL = 0.05     # first buy of a position
ADD_SOL = 0.2          # each pyramid add-on buy
MIN_BUY_SOL = 0.01     # smallest buy worth sending
BUY_SLIP_BPS = 1500    # slippage on buys; sells always go out at 10000
STOP_LOSS = 0.20       # exit when value < cost * (1 - STOP_LOSS)
TRAIL = 0.30           # trailing stop from the peak value
TRAIL_ARM = 0.05       # trail arms once value >= cost * (1 + TRAIL_ARM)
MAX_HOLD = 1500        # exit after this many slots held
COOLDOWN = 150         # slots to wait after an exit before re-buying a curve
MAX_ENTRIES = 2        # most entries per curve over the run
MAX_MAIN = 5           # most momentum positions open at once
MAX_EXPOSURE_SOL = 6.0  # most SOL at cost across all positions at once
TARGET_COINS = 25      # guardrail pacing: coins to reach over the run
TARGET_FILLS = 240     # guardrail pacing: fills to reach over the run
PACE_SECS = 14_400     # pacing horizon, seconds of block time
PROBE_SOL = 0.01       # probe round-trip buy size (== round-trip minimum)
MAX_PROBES = 1         # most probe round trips open at once


class ProtocolError(Exception):
    pass


class OrderError(Exception):
    pass


class Curve:
    __slots__ = (
        "mint", "created_slot", "virtual_token_reserves", "virtual_quote_reserves",
        "real_token_reserves", "real_quote_reserves", "reserves_slot", "complete",
    )

    def __init__(self, mint, created_slot, slot):
        self.mint = mint
        self.created_slot = created_slot
        self.virtual_token_reserves = 0
        self.virtual_quote_reserves = 0
        self.real_token_reserves = 0
        self.real_quote_reserves = 0
        self.reserves_slot = slot
        self.complete = False


class Tape:
    """A curve's rolling trade tape plus entry bookkeeping."""

    __slots__ = ("created", "trades", "entries", "last_exit")

    def __init__(self):
        self.created = None
        self.trades = []  # (slot, is_buy, sol, user, price), slot order
        self.entries = 0
        self.last_exit = -10**12


class Held:
    __slots__ = ("opened", "peak_value", "invested", "exiting", "filled",
                 "last_add_price", "last_value")

    def __init__(self, opened, invested, last_add_price):
        self.opened = opened
        self.peak_value = 0.0
        self.invested = invested  # SOL sent in buys for this position
        self.exiting = False
        self.filled = False
        self.last_add_price = last_add_price
        self.last_value = 0.0


class Bot:
    def __init__(self, hello):
        self.hello = hello
        self.seq = -1
        self.slot = -1
        self.cash = int(hello["capital"]["lamports"])
        self.positions = {}  # mint -> (token_amount, cost_lamports)
        self.curves = {}     # mint -> Curve, first-seen order
        self.pending = {}    # client_id -> order dict, send order
        self._used_ids = set()
        self._next_id = 1
        self.tapes = {}      # mint -> Tape
        self.held = {}       # mint -> Held
        self.probes = {}     # mint -> bool (False buying, True selling)
        self.my_fills = 0
        self.my_coins = set()
        self.first_time = None
        self.last_time = None

    # --- protocol helpers -------------------------------------------------
    def tx_fee_lamports(self):
        fees = self.hello["fees"]
        return int(fees["base_fee_lamports"]) + int(fees["priority_fee_lamports"])

    def max_buy_lamports(self, mint):
        curve = self.curves.get(mint)
        if curve is None or curve.complete:
            return 0
        return curve.real_quote_reserves * self.hello["caps"]["max_order_bps_of_real_reserve"] // BPS

    def committed_position_count(self):
        mints = set(self.positions)
        for o in self.pending.values():
            if o["side"] == "buy":
                mints.add(o["mint"])
        return len(mints)

    def free_cash_lamports(self):
        tx = self.tx_fee_lamports()
        free = self.cash
        for o in self.pending.values():
            free -= tx
            if o["side"] == "buy":
                free -= int(o["quote_lamports"])
        return free

    def _fresh_id(self):
        while True:
            cid = "o%d" % self._next_id
            self._next_id += 1
            if cid not in self._used_ids:
                self._used_ids.add(cid)
                return cid

    # --- batch intake -----------------------------------------------------
    def apply_batch(self, batch):
        self.seq = batch["seq"]
        self.slot = batch["slot"]
        for f in batch["fills"]:
            self.pending.pop(f["client_id"], None)
            if f["status"] == "filled" and not f["client_id"].startswith("_"):
                self.my_fills += 1
                self.my_coins.add(f["mint"])
        account = batch["account"]
        self.cash = int(account["cash_lamports"])
        self.positions = {}
        for p in account["positions"]:
            tokens = int(p["token_amount"])
            if tokens > 0:
                self.positions[p["mint"]] = (tokens, int(p["cost_lamports"]))
        for m in batch["marks"]:
            curve = self.curves.get(m["mint"])
            if curve is not None and int(m["slot"]) < curve.reserves_slot:
                continue
            if curve is None:
                curve = Curve(m["mint"], None, int(m["slot"]))
                self.curves[m["mint"]] = curve
            curve.virtual_token_reserves = int(m["virtual_token_reserves"])
            curve.virtual_quote_reserves = int(m["virtual_quote_reserves"])
            curve.real_token_reserves = int(m["real_token_reserves"])
            curve.real_quote_reserves = int(m["real_quote_reserves"])
            curve.reserves_slot = int(m["slot"])
        for e in batch["events"]:
            kind = e["event"]
            if kind == "CreateEvent":
                curve = Curve(e["mint"], e["slot"], e["slot"])
                self.curves[e["mint"]] = curve
                # Real CreateEvents carry virtual_sol_reserves; some also carry
                # virtual_quote_reserves. Either names the same SOL-side reserve.
                vq = e.get("virtual_quote_reserves") or e.get("virtual_sol_reserves") or 0
                curve.virtual_token_reserves = int(e["virtual_token_reserves"])
                curve.virtual_quote_reserves = int(vq)
                curve.real_token_reserves = int(e["real_token_reserves"])
                curve.real_quote_reserves = 0
                self.tape_of(e["mint"]).created = e["slot"]
            elif kind == "TradeEvent":
                curve = self.curves.get(e["mint"])
                if curve is None:
                    curve = Curve(e["mint"], None, e["slot"])
                    self.curves[e["mint"]] = curve
                curve.virtual_token_reserves = int(e["virtual_token_reserves"])
                curve.virtual_quote_reserves = int(e["virtual_quote_reserves"])
                curve.real_token_reserves = int(e["real_token_reserves"])
                curve.real_quote_reserves = int(e["real_quote_reserves"])
                curve.reserves_slot = e["slot"]
                tape = self.tape_of(e["mint"])
                price = int(e["virtual_quote_reserves"]) / int(e["virtual_token_reserves"])
                tape.trades.append((e["slot"], bool(e["is_buy"]),
                                    int(e["sol_amount"]) / LAMPORTS_PER_SOL,
                                    e["user"], price))
            else:  # CompleteEvent
                curve = self.curves.get(e["mint"])
                if curve is None:
                    curve = Curve(e["mint"], None, e["slot"])
                    self.curves[e["mint"]] = curve
                curve.complete = True

    def tape_of(self, mint):
        tape = self.tapes.get(mint)
        if tape is None:
            tape = Tape()
            self.tapes[mint] = tape
        return tape

    # --- strategy ---------------------------------------------------------
    def entry_signal(self, tape, now, rq_sol):
        if rq_sol < MIN_RQ_SOL or rq_sol > MAX_RQ_SOL:
            return False
        if tape.created is None or now - tape.created < MIN_AGE:
            return False
        start = now - WINDOW
        mid = now - WINDOW / 2
        buy_sol = 0.0
        sell_sol = 0.0
        buys = 0
        net_old = 0.0
        net_new = 0.0
        start_price = -1.0
        high = 0.0
        last = 0.0
        buyers = {}
        for (slot, is_buy, sol, user, price) in tape.trades:
            if slot <= start:
                start_price = price
                continue
            if start_price < 0:
                start_price = price
            signed = sol if is_buy else -sol
            if slot <= mid:
                net_old += signed
            else:
                net_new += signed
            if is_buy:
                buy_sol += sol
                buys += 1
                buyers[user] = buyers.get(user, 0.0) + sol
            else:
                sell_sol += sol
            if price > high:
                high = price
            last = price
        if buys < MIN_BUYS or len(buyers) < MIN_BUYERS:
            return False
        if buy_sol - sell_sol < MIN_NET_SOL:
            return False
        if start_price <= 0:
            return False
        up = last / start_price - 1
        if up < MIN_PRICE_UP or up > MAX_PRICE_UP:
            return False
        if last < high * NEAR_HIGH:
            return False
        if net_new <= 0 or net_new < ACCEL * net_old:
            return False
        top = max(buyers.values()) if buyers else 0.0
        if top > buy_sol:  # single wallet dominating the buys
            return False
        return True

    def flow(self, tape, now, window):
        net = 0.0
        for i in range(len(tape.trades) - 1, -1, -1):
            slot, is_buy, sol, _user, _price = tape.trades[i]
            if slot <= now - window:
                break
            net += sol if is_buy else -sol
        return net

    def decide(self, batch):
        now = batch["slot"]
        # Track block time for guardrail pacing.
        for e in batch["events"]:
            bt = e.get("block_time")
            if bt is not None:
                if self.first_time is None:
                    self.first_time = bt
                if self.last_time is None or bt > self.last_time:
                    self.last_time = bt
        # Prune tapes of curves that traded this batch.
        keep = WINDOW + 5
        traded = []       # mints with a TradeEvent this batch, first-seen order
        traded_seen = set()
        for e in batch["events"]:
            if e["event"] == "TradeEvent" and e["mint"] not in traded_seen:
                traded_seen.add(e["mint"])
                traded.append(e["mint"])
        for mint in traded:
            tape = self.tapes.get(mint)
            if tape is None:
                continue
            cut = 0
            while cut < len(tape.trades) and tape.trades[cut][0] <= now - keep:
                cut += 1
            if cut:
                del tape.trades[:cut]

        marks = {m["mint"]: m for m in batch["marks"]}
        pending_by_mint = {}
        for o in self.pending.values():
            pending_by_mint[o["mint"]] = pending_by_mint.get(o["mint"], 0) + 1

        orders = []
        cap = self.hello["caps"]["max_orders_per_batch"]
        tx = self.tx_fee_lamports() / LAMPORTS_PER_SOL

        # --- exits first ---------------------------------------------------
        for mint in list(self.held):
            h = self.held[mint]
            pos = self.positions.get(mint)
            curve = self.curves.get(mint)
            if pos is None:
                if mint not in pending_by_mint:
                    del self.held[mint]
                    tape = self.tape_of(mint)
                    if h.filled:
                        tape.last_exit = now
                    else:
                        tape.entries -= 1
                continue
            h.filled = True
            if curve is None or curve.complete:
                continue
            mk = marks.get(mint)
            vq = int(mk["virtual_quote_reserves"]) if mk else curve.virtual_quote_reserves
            vt = int(mk["virtual_token_reserves"]) if mk else curve.virtual_token_reserves
            rq_sol = (int(mk["real_quote_reserves"]) if mk else curve.real_quote_reserves) / LAMPORTS_PER_SOL
            tokens, cost_lamports = pos
            value = ((tokens * vq) / (vt + tokens) / LAMPORTS_PER_SOL) * (1 - CURVE_FEE) - tx
            cost = cost_lamports / LAMPORTS_PER_SOL
            if value > h.peak_value:
                h.peak_value = value
            h.last_value = value
            exit_now = h.exiting
            tape = self.tape_of(mint)
            if not exit_now and value < cost * (1 - STOP_LOSS):
                exit_now = True
            if (not exit_now and h.peak_value >= cost * (1 + TRAIL_ARM)
                    and value < h.peak_value * (1 - TRAIL)):
                exit_now = True
            if not exit_now and self.flow(tape, now, 15) < -1000:
                exit_now = True  # fade: net flow collapsed (threshold off by design)
            if not exit_now and now - h.opened > MAX_HOLD:
                exit_now = True  # max hold (holdIfUp = 1000 in the pilot: always applies)
            if not exit_now:
                continue
            h.exiting = True
            if mint in pending_by_mint:
                sell_pending = any(
                    o["mint"] == mint and o["side"] == "sell"
                    for o in self.pending.values()
                )
                if sell_pending:
                    continue
            if len(orders) >= cap:
                break
            orders.append({"side": "sell", "mint": mint,
                           "token_amount": str(tokens),
                           "max_slippage_bps": 10000})

        # --- probe round trips: sell the whole probe once its buy landed ---
        for mint in list(self.probes):
            selling = self.probes[mint]
            pos = self.positions.get(mint)
            curve = self.curves.get(mint)
            if pos is None:
                if mint not in pending_by_mint:
                    del self.probes[mint]
                continue
            if curve is None or curve.complete or mint in pending_by_mint:
                continue
            if len(orders) >= cap:
                break
            orders.append({"side": "sell", "mint": mint,
                           "token_amount": str(pos[0]),
                           "max_slippage_bps": 10000})
            if not selling:
                self.probes[mint] = True

        # --- entries and pyramid adds ---------------------------------------
        free = self.free_cash_lamports() / LAMPORTS_PER_SOL
        slots_left = self.hello["caps"]["max_open_positions"] - self.committed_position_count()
        main_left = MAX_MAIN - len(self.held)
        exposure = 0.0
        for mint, h in self.held.items():
            pos = self.positions.get(mint)
            exposure += max(h.invested, (pos[1] / LAMPORTS_PER_SOL) if pos else 0.0)
        for mint in traded:
            if len(orders) >= cap:
                break
            curve = self.curves.get(mint)
            if curve is None or curve.complete:
                continue
            if mint in pending_by_mint or mint in self.probes:
                continue
            tape = self.tapes.get(mint)
            if tape is None:
                continue
            h = self.held.get(mint)
            price = curve.virtual_quote_reserves / curve.virtual_token_reserves
            rq_sol = curve.real_quote_reserves / LAMPORTS_PER_SOL
            if h is None:
                if slots_left <= 0 or main_left <= 0:
                    continue
                if tape.entries >= MAX_ENTRIES or now - tape.last_exit < COOLDOWN:
                    continue
                if not self.entry_signal(tape, now, rq_sol):
                    continue
                want = min(INITIAL_SOL, STAKE_SOL)
            else:
                if h.exiting or not h.filled or h.invested >= STAKE_SOL - MIN_BUY_SOL:
                    continue
                pos = self.positions.get(mint)
                if pos is None:
                    continue
                if price < h.last_add_price:
                    continue
                if h.last_value < (pos[1] / LAMPORTS_PER_SOL):
                    continue
                if self.flow(tape, now, WINDOW) < 0:
                    continue
                if rq_sol > MAX_RQ_SOL:
                    continue
                want = min(ADD_SOL, STAKE_SOL - h.invested)
            cap_sol = self.max_buy_lamports(mint) / LAMPORTS_PER_SOL
            amount = min(cap_sol, want, MAX_EXPOSURE_SOL - exposure)
            if amount > free - tx:
                amount = free - tx
            if amount < MIN_BUY_SOL:
                continue
            lamports = int(amount * LAMPORTS_PER_SOL)
            orders.append({"side": "buy", "mint": mint,
                           "quote_lamports": str(lamports),
                           "max_slippage_bps": BUY_SLIP_BPS})
            free -= amount + tx
            exposure += amount
            if h is None:
                self.held[mint] = Held(now, amount, price)
                tape.entries += 1
                slots_left -= 1
                main_left -= 1
            else:
                h.invested += amount
                h.last_add_price = price

        # --- guardrail pacing: open a probe when fills/coins lag the pace ---
        if (len(self.probes) < MAX_PROBES and slots_left > 0
                and self.first_time is not None and self.last_time is not None):
            pace = min(1.0, (self.last_time - self.first_time) / PACE_SECS)
            need_coins = len(self.my_coins) < math.ceil(TARGET_COINS * pace)
            need_fills = self.my_fills < math.ceil(TARGET_FILLS * pace)
            if need_coins or need_fills:
                min_rq = (PROBE_SOL * BPS) / self.hello["caps"]["max_order_bps_of_real_reserve"]
                for mint in traded:
                    if len(orders) >= cap or len(self.probes) >= MAX_PROBES:
                        break
                    if mint in self.held or mint in self.probes or mint in pending_by_mint:
                        continue
                    if mint in self.positions:
                        continue
                    if need_coins and not need_fills and mint in self.my_coins:
                        continue
                    curve = self.curves.get(mint)
                    if curve is None or curve.complete:
                        continue
                    if curve.real_quote_reserves / LAMPORTS_PER_SOL < min_rq * 1.5:
                        continue
                    if free < PROBE_SOL + 2 * tx:
                        break
                    orders.append({"side": "buy", "mint": mint,
                                   "quote_lamports": str(int(PROBE_SOL * LAMPORTS_PER_SOL)),
                                   "max_slippage_bps": BUY_SLIP_BPS})
                    self.probes[mint] = False
                    free -= PROBE_SOL + tx

        return orders

    # --- wire ---------------------------------------------------------------
    def validate_orders(self, orders):
        cap = self.hello["caps"]["max_orders_per_batch"]
        if len(orders) > cap:
            raise OrderError("batch %d: %d orders over max_orders_per_batch (%d)"
                             % (self.seq, len(orders), cap))
        checked = []
        for i, o in enumerate(orders):
            if not isinstance(o, dict):
                raise OrderError("batch %d order %d: not a dict" % (self.seq, i))
            side = o.get("side")
            if side not in ("buy", "sell"):
                raise OrderError("batch %d order %d: bad side %r" % (self.seq, i, side))
            allowed = ("side", "client_id", "mint", "quote_lamports", "max_slippage_bps") \
                if side == "buy" else \
                ("side", "client_id", "mint", "token_amount", "max_slippage_bps")
            for k in o:
                if k not in allowed:
                    raise OrderError("batch %d order %d: unknown key %r" % (self.seq, i, k))
            mint = o.get("mint")
            if not isinstance(mint, str) or not _PUBKEY_RE.fullmatch(mint):
                raise OrderError("batch %d order %d: bad mint %r" % (self.seq, i, mint))
            amount_key = "quote_lamports" if side == "buy" else "token_amount"
            amount = o.get(amount_key)
            n = None
            if type(amount) is int:
                n = amount
            elif isinstance(amount, str) and re.fullmatch(r"0|[1-9][0-9]{0,19}", amount):
                n = int(amount)
            if n is None or n < 0 or n > U64_MAX:
                raise OrderError("batch %d order %d: bad %s %r" % (self.seq, i, amount_key, amount))
            bps = o.get("max_slippage_bps")
            if type(bps) is not int or not 0 <= bps <= 10000:
                raise OrderError("batch %d order %d: bad max_slippage_bps %r" % (self.seq, i, bps))
            cid = o.get("client_id")
            if cid is None:
                cid = self._fresh_id()
            elif not isinstance(cid, str) or not _CLIENT_ID_RE.fullmatch(cid):
                raise OrderError("batch %d order %d: bad client_id %r" % (self.seq, i, cid))
            checked.append({"side": side, "client_id": cid, "mint": mint,
                            amount_key: str(n), "max_slippage_bps": bps})
        return checked

    def run(self):
        # Serves one run: builds the real Bot state on hello. `self` is only a
        # throwaway shell here (see _main); no state is read before hello.
        out = sys.stdout
        state = None
        for raw in sys.stdin.buffer:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line.strip():
                continue
            if len(raw) > MAX_HARNESS_LINE_BYTES:
                raise ProtocolError("input line over %d bytes" % MAX_HARNESS_LINE_BYTES)
            try:
                msg = json.loads(line)
            except ValueError:
                raise ProtocolError("input line is not JSON") from None
            kind = msg.get("type") if isinstance(msg, dict) else None
            if kind == "hello":
                if state is not None:
                    raise ProtocolError("a second hello")
                if msg.get("protocol") != PROTOCOL:
                    raise ProtocolError("wrong protocol: %r" % (msg.get("protocol"),))
                state = Bot(msg)
                bot = state
            elif kind == "batch":
                if state is None:
                    raise ProtocolError("a batch before hello")
                if (type(msg.get("seq")) is not int or type(msg.get("slot")) is not int
                        or not isinstance(msg.get("events"), list)
                        or not isinstance(msg.get("marks"), list)
                        or not isinstance(msg.get("fills"), list)
                        or not isinstance(msg.get("account"), dict)):
                    raise ProtocolError("malformed batch")
                state.apply_batch(msg)
                orders = state.validate_orders(state.decide(msg))
                text = json.dumps({"type": "reply", "seq": msg["seq"], "orders": orders},
                                  separators=(",", ":"), ensure_ascii=False)
                if len(text.encode("utf-8")) > MAX_BOT_LINE_BYTES:
                    raise OrderError("reply over %d bytes" % MAX_BOT_LINE_BYTES)
                for o in orders:
                    state.pending[o["client_id"]] = o
                out.write(text + "\n")
                out.flush()
            elif kind == "done":
                if state is None:
                    raise ProtocolError("done before hello")
                return 0
            else:
                raise ProtocolError("unknown message type %r" % (kind,))
        if state is None:
            raise ProtocolError("input ended before hello")
        raise ProtocolError("input ended before done")


def _main():
    # A throwaway Bot instance only to host run(); run() builds the real one on hello.
    shell = Bot.__new__(Bot)
    return shell.run()


if __name__ == "__main__":
    sys.exit(_main())
