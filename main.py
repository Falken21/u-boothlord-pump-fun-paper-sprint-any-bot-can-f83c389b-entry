#!/usr/bin/env python3
"""Boothlord Pump.fun Paper Sprint bot -- v2.

Strategy in one paragraph: guardrail first, momentum second, capital always.
A probe engine guarantees the 20-coin round-trip guardrail by cycling tiny
~0.012 SOL round trips on every mint with real liquidity, front-loaded and
never blocked by the momentum filters.  A deliberately loose momentum sleeve
then trades small stakes (<= 0.4 SOL per mint) on curves with fresh net
inflow, and every position is cut fast: -12% stop-loss, an early-armed
trailing stop, a 600-slot max hold and a half-off take-profit at 2x.

Protocol: agent-fun/replay/v2 -- one long-lived process, newline-delimited
JSON on stdin/stdout.  Reads `hello` once, answers every `batch` with exactly
one `reply` echoing its `seq`, exits on `done`.  Stdout carries replies only.

Determinism: no wall clock, no randomness, no environment reads; all
iteration that can affect output order is over insertion-ordered dicts or
explicitly sorted keys, and every threshold is a pure function of the event
stream.
"""

import json
import sys
from collections import deque

# ---------------------------------------------------------------------------
# Constants (lamports unless noted).  Run parameters from `hello` override
# the fee/cap defaults below when the harness provides them.
# ---------------------------------------------------------------------------

TX_FEE_DEFAULT = 105_000            # 5,000 base + 100,000 priority
MIN_ROUND_TRIP_BUY = 10_000_000     # guardrail: buy fills >= 0.01 SOL per coin

# Probe engine (guardrail).
PROBE_TARGET_COUNT = 30             # counted round trips to aim for (>= 20 req)
PROBE_TARGET_BUYS = 12_000_000      # cumulative buy fills per probe mint
PROBE_MIN_RESERVE = 55_000_000      # real SOL reserve floor to start a probe
PROBE_MIN_PER_REPLY = 2_500_000     # smallest probe tranche worth its tx fee
PROBE_SELL_AGE = 6                  # slots after last buy fill before selling
PROBE_GIVE_UP_AGE = 90              # sell an under-filled probe anyway by then
PROBE_MAX_ATTEMPTS = 4              # failed/rejected probe buys before giving up
PROBE_RECENT_TRADE = 6_000          # mint must have traded within this window

# Momentum sleeve.
MOM_MAX_STAKE = 350_000_000         # 0.35 SOL total buys per mint, hard cap
MOM_FIRST_BUY = 60_000_000          # desired first buy (reserve cap may shrink)
MOM_MIN_BUY = 15_000_000            # never send a momentum buy below this
MOM_ADD_BUY = 120_000_000           # the single allowed pyramid add
MOM_MIN_AGE = 60                    # curve age in slots before entry
MOM_MIN_TRADES = 10                 # observed trades before entry
MOM_INFLOW_WINDOW = 60              # slots for the inflow test
MOM_INFLOW_MIN = 400_000_000        # net inflow over the window, lamports
MOM_BUYVOL_MIN = 700_000_000        # alternative: raw buy volume in window
MOM_BUY_SHARE_MIN = 0.60            # ... with at least this buy share
MOM_MIN_BUYERS = 4                  # distinct buyers inside the window
MOM_LAG = 120                       # momentum lookback in slots
MOM_MIN_GAIN = 0.03                 # price must be up >= 3% over the lag
MOM_MAX_VS_BASE = 1.60              # skip if price > 1.6x the recent base low
MOM_BASE_WINDOW = 300               # slots for the base-low calculation
MOM_MIN_RESERVE = 150_000_000       # real SOL reserve floor for entries
MOM_COOLDOWN = 300                  # slots after a full exit before re-entry
MOM_EVAL_EVERY = 10                 # min slots between evaluations per mint

# Exits.
STOP_LOSS_FRAC = 0.12               # sell all at -12% vs average cost
TRAIL_ARM_FRAC = 0.08               # trailing stop arms at +8%
TRAIL_DROP_FRAC = 0.15              # ... and fires 15% below the peak
MAX_HOLD_SLOTS = 600                # time stop for momentum positions
TAKE_PROFIT_MULT = 2.0              # sell half when price doubles

# Cash / tape discipline.
DD_HALT_FRAC = 0.35                 # halt momentum entries at this drawdown
CASH_BUFFER = 30_000_000            # lamports always left uncommitted
TAPE_KEEP_SLOTS = 900               # per-mint trade tape retention
TAPE_MAX_TRADES = 400               # per-mint tape length cap

# Slippage tolerances (bps).
SLIP_BUY = 1_000
SLIP_EXIT = 3_000
SLIP_PROBE_SELL = 10_000


# ---------------------------------------------------------------------------
# Per-mint state
# ---------------------------------------------------------------------------

class Mint(object):
    __slots__ = (
        "created", "alive", "vsol", "vtok", "rsol", "rtok",
        "last_trade_slot", "total_trades", "tape",
        "buy_fills", "total_buys", "stake_mom",
        "probe_stage", "probe_attempts", "probe_last_fill", "in_queue",
        "counted", "last_exit_slot", "mom_eval_slot",
        "open_slot", "peak_price", "tp_done", "added",
    )

    def __init__(self, created):
        self.created = created
        self.alive = True
        self.vsol = 0
        self.vtok = 0
        self.rsol = 0
        self.rtok = 0
        self.last_trade_slot = -1
        self.total_trades = 0
        self.tape = deque()          # (slot, signed_sol, is_buy, user, price)
        self.buy_fills = 0           # cumulative filled buy quote (lamports)
        self.total_buys = 0          # same, kept for the stake cap
        self.stake_mom = 0           # momentum buys only
        self.probe_stage = "none"    # none | accumulating | holding
        self.probe_attempts = 0
        self.probe_last_fill = -1
        self.in_queue = False
        self.counted = False
        self.last_exit_slot = -1
        self.mom_eval_slot = -1
        # Live-position bookkeeping (meaningful only while held).
        self.open_slot = -1
        self.peak_price = 0.0
        self.tp_done = False
        self.added = False

    def price(self):
        if self.vtok > 0:
            return self.vsol / self.vtok
        return 0.0


def _reserves(ev):
    """Pull (virtual_sol, virtual_token, real_sol, real_token) from an event
    or mark, tolerating both the *_sol_* and *_quote_* field spellings."""
    vsol = ev.get("virtual_quote_reserves")
    if vsol is None:
        vsol = ev.get("virtual_sol_reserves")
    rsol = ev.get("real_quote_reserves")
    if rsol is None:
        rsol = ev.get("real_sol_reserves")
    vtok = ev.get("virtual_token_reserves")
    rtok = ev.get("real_token_reserves")
    return (
        int(vsol) if vsol is not None else 0,
        int(vtok) if vtok is not None else 0,
        int(rsol) if rsol is not None else 0,
        int(rtok) if rtok is not None else 0,
    )


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class Bot(object):
    def __init__(self):
        self.tx_fee = TX_FEE_DEFAULT
        self.max_positions = 5
        self.max_orders = 16
        self.reserve_bps = 100       # per-mint per-reply buy cap, bps of rsol
        self.cash = 0
        self.positions = {}          # mint -> (token_amount, cost_lamports)
        self.mints = {}              # mint -> Mint
        self.pending = {}            # client_id -> (mint, side, amount, kind)
        self.pending_by_mint = {}    # mint -> set of client_ids
        self.probe_queue = deque()
        self.probe_active = set()    # mints with a probe in flight (O(1) size)
        self.counted_count = 0
        self.peak_equity = 0
        self.halt_momentum = False
        self.order_seq = 0
        self.traded_this_batch = []

    # -- pending bookkeeping ------------------------------------------------

    def _pending_add(self, cid, mint, side, amount, kind):
        self.pending[cid] = (mint, side, amount, kind)
        s = self.pending_by_mint.get(mint)
        if s is None:
            s = set()
            self.pending_by_mint[mint] = s
        s.add(cid)

    def _pending_drop(self, cid):
        rec = self.pending.pop(cid, None)
        if rec is not None:
            s = self.pending_by_mint.get(rec[0])
            if s is not None:
                s.discard(cid)
                if not s:
                    del self.pending_by_mint[rec[0]]
        return rec

    def _has_pending(self, mint, side=None):
        s = self.pending_by_mint.get(mint)
        if not s:
            return False
        if side is None:
            return True
        for cid in s:
            if self.pending[cid][1] == side:
                return True
        return False

    def _free_cash(self):
        committed = 0
        for _mint, side, amount, _kind in self.pending.values():
            committed += self.tx_fee
            if side == "buy":
                committed += amount
        return self.cash - committed

    def _committed_positions(self):
        n = len(self.positions)
        for mint in self.pending_by_mint:
            if mint in self.positions:
                continue
            for cid in self.pending_by_mint[mint]:
                if self.pending[cid][1] == "buy":
                    n += 1
                    break
        return n

    def _reserve_cap(self, mint):
        m = self.mints[mint]
        return m.rsol * self.reserve_bps // 10_000

    # -- input processing ---------------------------------------------------

    def on_hello(self, msg):
        fees = msg.get("fees") or {}
        base = fees.get("base_fee_lamports")
        prio = fees.get("priority_fee_lamports")
        if base is not None and prio is not None:
            self.tx_fee = int(base) + int(prio)
        caps = msg.get("caps") or {}
        if caps.get("max_open_positions") is not None:
            self.max_positions = int(caps["max_open_positions"])
        if caps.get("max_orders_per_batch") is not None:
            self.max_orders = int(caps["max_orders_per_batch"])
        if caps.get("max_order_bps_of_real_reserve") is not None:
            self.reserve_bps = int(caps["max_order_bps_of_real_reserve"])
        cap = msg.get("capital")
        if isinstance(cap, dict):
            cap = cap.get("lamports")
        if cap is not None:
            self.cash = int(cap)

    def _process_fills(self, fills):
        for f in fills:
            cid = f.get("client_id")
            if cid is None or cid.startswith("_"):
                continue
            rec = self._pending_drop(cid)
            if rec is None:
                continue
            mint, side, _amount, kind = rec
            m = self.mints.get(mint)
            if m is None:
                continue
            if f.get("status") == "filled":
                if side == "buy":
                    q = int(f.get("quote_lamports") or 0)
                    m.buy_fills += q
                    m.total_buys += q
                    if kind == "mom" or kind == "add":
                        m.stake_mom += q
                    if kind == "probe":
                        m.probe_last_fill = int(f.get("slot") or 0)
                        if m.buy_fills >= MIN_ROUND_TRIP_BUY:
                            m.probe_stage = "holding"
                # Filled sells need no bookkeeping: the account snapshot
                # next batch reflects the new position.
            else:
                # failed / rejected: unwind the intent so it can retry.
                if kind == "probe":
                    m.probe_stage = "none"
                    m.probe_attempts += 1
                    self.probe_active.discard(mint)
                elif kind == "add":
                    m.added = False

    def _process_account(self, account, slot):
        if account is None:
            return
        self.cash = int(account.get("cash_lamports") or 0)
        new_positions = {}
        for p in account.get("positions") or []:
            new_positions[p["mint"]] = (
                int(p.get("token_amount") or 0),
                int(p.get("cost_lamports") or 0),
            )
        old = self.positions
        # Closures: a mint held last batch and gone now has completed its
        # round trip (our sell, a curve completion or the final flatten).
        for mint in old:
            if mint not in new_positions:
                m = self.mints.get(mint)
                if m is not None:
                    if not m.counted and m.buy_fills >= MIN_ROUND_TRIP_BUY:
                        m.counted = True
                        self.counted_count += 1
                    m.last_exit_slot = slot
                    m.open_slot = -1
                    m.probe_stage = "none"
                    self.probe_active.discard(mint)
        # Openings: initialise live-position bookkeeping.
        for mint in new_positions:
            if mint not in old:
                m = self.mints.get(mint)
                if m is not None:
                    m.open_slot = slot
                    m.peak_price = m.price()
                    m.tp_done = False
                    if m.stake_mom == 0:
                        m.added = False
        self.positions = new_positions

    def _process_events(self, events, slot):
        self.traded_this_batch = []
        seen = set()
        for ev in events:
            kind = ev.get("event")
            mint = ev.get("mint")
            if mint is None:
                continue
            m = self.mints.get(mint)
            if kind == "CreateEvent":
                if m is None:
                    m = Mint(slot)
                    self.mints[mint] = m
                vsol, vtok, rsol, rtok = _reserves(ev)
                if vsol:
                    m.vsol = vsol
                if vtok:
                    m.vtok = vtok
                if rsol:
                    m.rsol = rsol
                if rtok:
                    m.rtok = rtok
                continue
            if m is None:
                m = Mint(slot)
                self.mints[mint] = m
            if kind == "CompleteEvent":
                m.alive = False
                continue
            if kind != "TradeEvent":
                continue
            vsol, vtok, rsol, rtok = _reserves(ev)
            m.vsol, m.vtok, m.rsol, m.rtok = vsol, vtok, rsol, rtok
            m.last_trade_slot = slot
            m.total_trades += 1
            sol = int(ev.get("sol_amount") or 0)
            is_buy = bool(ev.get("is_buy"))
            signed = sol if is_buy else -sol
            m.tape.append((slot, signed, is_buy, ev.get("user") or "", m.price()))
            cutoff = slot - TAPE_KEEP_SLOTS
            while m.tape and (m.tape[0][0] < cutoff or len(m.tape) > TAPE_MAX_TRADES):
                m.tape.popleft()
            if mint not in seen:
                seen.add(mint)
                self.traded_this_batch.append(mint)
            self._maybe_queue_probe(mint, slot)

    def _process_marks(self, marks):
        for mk in marks or []:
            mint = mk.get("mint")
            m = self.mints.get(mint)
            if m is None:
                continue
            vsol, vtok, rsol, rtok = _reserves(mk)
            if vtok:
                m.vsol, m.vtok, m.rsol, m.rtok = vsol, vtok, rsol, rtok
            if mint in self.positions:
                p = m.price()
                if p > m.peak_price:
                    m.peak_price = p

    # -- probe engine ---------------------------------------------------------

    def _maybe_queue_probe(self, mint, slot):
        m = self.mints[mint]
        if (
            m.counted
            or m.in_queue
            or m.probe_stage != "none"
            or m.probe_attempts >= PROBE_MAX_ATTEMPTS
            or m.buy_fills > 0
            or not m.alive
            or mint in self.positions
            or self._has_pending(mint, "buy")
            or m.rsol < PROBE_MIN_RESERVE
        ):
            return
        m.in_queue = True
        self.probe_queue.append(mint)

    def _probe_orders(self, slot, orders):
        # First, top up probes that are still accumulating buy fills: on a
        # shallow curve one reply's 1%-of-reserve cap buys less than the
        # 0.01 SOL a countable round trip needs, so tranches stack across
        # batches until the mint's buy fills clear the minimum.
        for mint in sorted(self.probe_active):
            if len(orders) >= self.max_orders:
                return
            m = self.mints[mint]
            if (
                m.probe_stage != "accumulating"
                or m.buy_fills >= PROBE_TARGET_BUYS
                or self._has_pending(mint, "buy")
                or not m.alive
            ):
                continue
            quote = min(PROBE_TARGET_BUYS - m.buy_fills, self._reserve_cap(mint))
            if quote < PROBE_MIN_PER_REPLY:
                continue
            if quote + self.tx_fee > self._free_cash() - 5_000_000:
                break
            self.order_seq += 1
            cid = "b%d" % self.order_seq
            orders.append({
                "side": "buy",
                "client_id": cid,
                "mint": mint,
                "quote_lamports": str(quote),
                "max_slippage_bps": SLIP_BUY,
            })
            self._pending_add(cid, mint, "buy", quote, "probe")
        in_flight = len(self.probe_active)
        while (
            len(orders) < self.max_orders
            and self.counted_count + in_flight < PROBE_TARGET_COUNT
            and self.probe_queue
        ):
            mint = self.probe_queue.popleft()
            m = self.mints[mint]
            m.in_queue = False
            if (
                m.counted
                or m.probe_stage != "none"
                or m.probe_attempts >= PROBE_MAX_ATTEMPTS
                or m.buy_fills > 0
                or not m.alive
                or mint in self.positions
                or self._has_pending(mint, "buy")
                or slot - m.last_trade_slot > PROBE_RECENT_TRADE
                or m.rsol < PROBE_MIN_RESERVE
            ):
                continue
            if self._committed_positions() >= self.max_positions:
                m.in_queue = True
                self.probe_queue.appendleft(mint)
                break
            quote = min(PROBE_TARGET_BUYS - m.buy_fills, self._reserve_cap(mint))
            if quote < PROBE_MIN_PER_REPLY:
                continue
            if quote + self.tx_fee > self._free_cash() - 5_000_000:
                break
            self.order_seq += 1
            cid = "b%d" % self.order_seq
            orders.append({
                "side": "buy",
                "client_id": cid,
                "mint": mint,
                "quote_lamports": str(quote),
                "max_slippage_bps": SLIP_BUY,
            })
            self._pending_add(cid, mint, "buy", quote, "probe")
            m.probe_stage = "accumulating"
            self.probe_active.add(mint)
            in_flight += 1

    # -- exits ----------------------------------------------------------------

    def _exit_orders(self, slot, orders):
        for mint in sorted(self.positions):
            if len(orders) >= self.max_orders:
                break
            if self._has_pending(mint, "sell"):
                continue
            m = self.mints.get(mint)
            if m is None:
                continue
            token_amount, cost = self.positions[mint]
            if token_amount <= 0:
                continue
            price = m.price()
            if price <= 0:
                continue
            entry = cost / token_amount if token_amount else 0.0
            sell_amount = 0
            slip = SLIP_EXIT
            if m.stake_mom == 0 and m.probe_stage in ("accumulating", "holding"):
                # Pure probe position: leave once the round trip is worth
                # counting (buy fills >= the guardrail minimum) and the
                # position has seasoned a few slots.  If the top-ups never
                # got it there, give up later and free the slot.
                last = m.probe_last_fill if m.probe_last_fill >= 0 else m.open_slot
                age = slot - last
                if m.buy_fills >= MIN_ROUND_TRIP_BUY and age >= PROBE_SELL_AGE:
                    sell_amount = token_amount
                    slip = SLIP_PROBE_SELL
                elif age >= PROBE_GIVE_UP_AGE:
                    sell_amount = token_amount
                    slip = SLIP_PROBE_SELL
            elif entry > 0:
                if price <= entry * (1.0 - STOP_LOSS_FRAC):
                    sell_amount = token_amount
                elif (
                    m.peak_price >= entry * (1.0 + TRAIL_ARM_FRAC)
                    and price <= m.peak_price * (1.0 - TRAIL_DROP_FRAC)
                ):
                    sell_amount = token_amount
                elif m.open_slot >= 0 and slot - m.open_slot >= MAX_HOLD_SLOTS:
                    sell_amount = token_amount
                elif not m.tp_done and price >= entry * TAKE_PROFIT_MULT:
                    sell_amount = token_amount // 2
                    m.tp_done = True
            if sell_amount > 0:
                self.order_seq += 1
                cid = "b%d" % self.order_seq
                orders.append({
                    "side": "sell",
                    "client_id": cid,
                    "mint": mint,
                    "token_amount": str(sell_amount),
                    "max_slippage_bps": slip,
                })
                self._pending_add(cid, mint, "sell", sell_amount, "sell")

    # -- momentum sleeve ------------------------------------------------------

    def _window_stats(self, m, slot):
        """(net_inflow, buy_volume, total_volume, distinct_buyers) over the
        inflow window, plus the price at the momentum lag and the window's
        base-low price."""
        net = 0
        buy_vol = 0
        tot_vol = 0
        buyers = set()
        lag_price = 0.0
        base_low = 0.0
        win_start = slot - MOM_INFLOW_WINDOW
        lag_slot = slot - MOM_LAG
        base_start = slot - MOM_BASE_WINDOW
        for t_slot, signed, is_buy, user, price in m.tape:
            if t_slot > win_start:
                net += signed
                tot_vol += abs(signed)
                if is_buy:
                    buy_vol += signed
                    buyers.add(user)
            if t_slot <= lag_slot:
                lag_price = price
            if t_slot > base_start and price > 0:
                if base_low == 0.0 or price < base_low:
                    base_low = price
        return net, buy_vol, tot_vol, len(buyers), lag_price, base_low

    def _momentum_orders(self, slot, orders):
        if self.halt_momentum:
            return  # drawdown circuit breaker: probes still run, risk off
        for mint in sorted(self.traded_this_batch):
            if len(orders) >= self.max_orders:
                break
            m = self.mints.get(mint)
            if m is None or not m.alive:
                continue
            if self._has_pending(mint, "buy"):
                continue
            if slot - m.mom_eval_slot < MOM_EVAL_EVERY:
                continue
            m.mom_eval_slot = slot
            held = mint in self.positions
            if held and m.stake_mom == 0:
                continue  # probe position: the probe engine owns it
            if not held:
                if m.counted and slot - m.last_exit_slot < MOM_COOLDOWN:
                    continue
                if m.last_exit_slot >= 0 and slot - m.last_exit_slot < MOM_COOLDOWN:
                    continue
                if m.probe_stage != "none":
                    continue
                if slot - m.created < MOM_MIN_AGE:
                    continue
                if m.total_trades < MOM_MIN_TRADES:
                    continue
                if m.rsol < MOM_MIN_RESERVE:
                    continue
                if self._committed_positions() >= self.max_positions:
                    continue
            net, buy_vol, tot_vol, buyers, lag_price, base_low = \
                self._window_stats(m, slot)
            price = m.price()
            if price <= 0 or lag_price <= 0 or base_low <= 0:
                continue
            inflow_ok = net >= MOM_INFLOW_MIN or (
                buy_vol >= MOM_BUYVOL_MIN
                and tot_vol > 0
                and buy_vol / tot_vol >= MOM_BUY_SHARE_MIN
            )
            if not inflow_ok or buyers < MOM_MIN_BUYERS:
                continue
            if price / lag_price - 1.0 < MOM_MIN_GAIN:
                continue
            if price > base_low * MOM_MAX_VS_BASE:
                continue  # parabolic top: do not chase
            room = MOM_MAX_STAKE - m.total_buys
            if room < MOM_MIN_BUY:
                continue
            if not held:
                quote = min(MOM_FIRST_BUY, self._reserve_cap(mint), room)
                kind = "mom"
            else:
                if m.added:
                    continue
                token_amount, cost = self.positions[mint]
                entry = cost / token_amount if token_amount else 0.0
                if entry <= 0 or price < entry * 1.10 or net <= 0:
                    continue
                quote = min(MOM_ADD_BUY, self._reserve_cap(mint), room)
                kind = "add"
            if quote < MOM_MIN_BUY:
                continue
            if quote + self.tx_fee > self._free_cash() - CASH_BUFFER:
                continue
            self.order_seq += 1
            cid = "b%d" % self.order_seq
            orders.append({
                "side": "buy",
                "client_id": cid,
                "mint": mint,
                "quote_lamports": str(quote),
                "max_slippage_bps": SLIP_BUY,
            })
            self._pending_add(cid, mint, "buy", quote, kind)
            if kind == "add":
                m.added = True

    # -- batch driver -----------------------------------------------------------

    def on_batch(self, msg):
        slot = int(msg.get("slot") or 0)
        self._process_fills(msg.get("fills") or [])
        self._process_account(msg.get("account"), slot)
        self._process_events(msg.get("events") or [], slot)
        self._process_marks(msg.get("marks") or [])
        # Equity estimate for the drawdown circuit breaker: cash plus the
        # curve value of every open position at current virtual reserves.
        equity = self.cash
        for mint, (token_amount, cost) in self.positions.items():
            m = self.mints.get(mint)
            if m is not None and m.vtok > 0 and token_amount > 0:
                equity += token_amount * m.vsol // (m.vtok + token_amount)
            else:
                equity += cost
        if equity > self.peak_equity:
            self.peak_equity = equity
        self.halt_momentum = (
            self.peak_equity > 0
            and equity < self.peak_equity * (1.0 - DD_HALT_FRAC)
        )
        orders = []
        self._exit_orders(slot, orders)
        self._probe_orders(slot, orders)
        self._momentum_orders(slot, orders)
        return {"type": "reply", "seq": msg.get("seq"), "orders": orders}


def main():
    bot = Bot()
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        mtype = msg.get("type")
        if mtype == "hello":
            bot.on_hello(msg)
        elif mtype == "batch":
            reply = bot.on_batch(msg)
            out.write(json.dumps(reply, separators=(",", ":")) + "\n")
            out.flush()
        elif mtype == "done":
            break


if __name__ == "__main__":
    main()
