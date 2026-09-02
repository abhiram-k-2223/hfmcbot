# Wallet Strategy Reverse-Engineering — Full Process Report

**Project:** whale-kol
**Date:** 2026-09-01
**Context:** Executing the @rektfencer methodology (tweet 2094427061800857919) on Solana via GMGN, then reverse-engineering the most viable bot wallet found along the way.

> Source methodology (rektfencer):
> 1) Pick a monster winner (100x/500x/1000x). You want who bought BEFORE everyone else.
> 2) Find the first buyers (GMGN for SOL).
> 3) Remove inactive wallets (30-day activity).
> 4) Remove bots (buy/sell within seconds).
> 5) Check what survivors buy now (recent buys, sizes, holds, sold).
> 6) Look for overlap (same token in 3+ proven wallets).
> 7) DYOR.

---

## Part 1 — Selecting the monster winner (Step 1)

**Chosen: ANSEM "The Black Bull"**
- CA (SOL): `9cRCn9rGT8V2imeM2BaKs13yhMEais3ruM3rPvTGpump`
- Launched **2026-06-16** on Pump.fun.
- Price action (daily kline): opened ~$0.000222 → bled sideways 06-17..06-26 → **exploded 06-27** (~$0.0118) → **06-28** (~$0.09) → ATH **~$0.449** on 07-06 (mcap ~$449M).
- ~**2000x** from open to ATH (genuine 100x/200x/500x-class winner).
- Current (09-01): ~$0.287, mcap ~$286M, 135k holders, 381 smart wallets, 138 KOLs.
- Security: `creator_close`, rug_ratio 0.24, CTO flag 1, top-10 holder ~65% (concentrated), on meteora_dlmm.

**The "move" started 06-27.** Early buyers = wallets holding *before* 06-27.

---

## Part 2 — Early buyers & filtering (Steps 2–4)

Universe: `gmgn-cli token traders --chain sol --address <CA> --order-by profit --limit 100`, filtered by `start_holding_at < 06-27`.

### Pre-move buyers (entered BEFORE the run)
| Wallet | Entry | P&L | 30-day activity | Tags | Filter |
|---|---|---|---|---|---|
| `9aP2…SR5T` | 06-16 (launch) | +$597K | 15 buys / 9 sells, active 08-29 | `arbitrager, axiom` | sniper/arb bot |
| `ACTb…Y831` | 06-20 | +$310K | 21,286 buys/mo | `photon, axiom` | high-freq bot → removed |
| `APgK…jk8vD` | 06-21 | +$390K | 663 buys, active 09-01 | `arbitrager` | arb wallet → kept |

### Post-move / named wallets (for context)
- `nyhrox` (6S8G…, `kol,smart_degen`) entered 06-27 (+$41K)
- `JADAWGS` (3H9L…) +$757K, active only 07-31; `Rune` +$343K (07-30); `chit🇰🇷` +$231K
- Nurse bot tags observed on winners: `axiom, photon, trojan, padre, gmgn, arbitrager, sandwich_bot`

### Key finding
On a hyper-fast SOL pump, the wallets that "found it early" are **almost exclusively sniper/arbitrage/terminal bots** (axiom/photon/arbitrager). Named/smart-money humans entered *after* the move. Steps 3–4 therefore filter out most of the early set — exactly as the methodology warns.

---

## Part 3 — What the surviving wallet buys now (Step 5)

**`APgK…jk8vD`** (the 06-21 pre-move winner) is **steadily accumulating MANLET**:
- Repeated buys 08-27 → 09-01 (~$140–$1,400 each), currently holding **13.8M MANLET (~$10.2K)**.
- Only the ANSEM pre-move set, this wallet holds MANLET → weak single-wallet signal (no multi-wallet overlap, Step 6 weak).

**MANLET** — SOL `HxQhDGYqyjorgogMJx7YbBHADEDxuHhLnMMmr6VYpyn`
- stonkfun launch, migrated 08-09; current ~$0.000738; liq $145K; 5,850 holders.
- 🟢 Security: mint+freeze renounced, LP burned, 0% tax, top-10 13.35%.
- ⚠️ **Pool quoted in ANSEM** (trades against the winner leg); 24h vol $546K; 162 smart + 26 KOL holders.
- ⚠️ Risk: dev `creator_hold`, bundler 35% / entrapment 54% / bot_degen 34%, CTO flag.

---

## Part 4 — Reverse-engineering the most viable bot: `CLM6E4…Kg1Q`

**Rationale for choice:** highest $ profit on the winner (+$2.7M), moderate frequency (studyable), and shows a clean winner→rotation pattern — the strongest signal-to-noise.

**Data captured:** 340 buys + 340 sells (08-01 → 08-28), 67 unique tokens, paginated via `portfolio activity`.

### Inferred strategy: "new-launch spray + let winners run" rotation bot

**1. Token selection — fresh launches only**
- Pump.fun (166 buys) + **stonkfun (118)** + meteora_virtual_curve (8). No established names.

**2. Entry timing — split-second funnel (signature)**
- **76% of buys occur in ≤5-second bursts** (105 bursts, mostly 2–3 per token).
- Same-second multi-buy = anti-slippage / position-split funneling (sniper/MEV-aware behavior).

**3. Sizing — spread, not whale**
- Median ~$1,342; 152 positions in the $1–5K band; capital spread across many launches.

**4. Hold behavior — two modes**
- Flip mode: median ~4.9h; 13 tokens <1h; 4 flipped <1min.
- Conviction mode: 9 tokens held ≥24h (up to 15 days, `DROYD`).

**5. Exit reality — the edge is 1–2 winners**
```
ANSEM   buy$10.2K → sell$131.6K   +$121,465   ← the entire edge
STONK   -$24.6K   SPYx -$42.7K   Kimchi -$37.9K   ANTHRP -$24.7K
MANLET  -$16.9K   (bought $48K, sold $31K)
```
- Net sampled P&L across closed tokens: **−$112K**, saved entirely by ANSEM.
- Losing positions are **cut fast** (small-loss tail); breakout winners are **let run**.
- 30-day winrate only ~25% → edge is *speed + max-winner*, not selection.

### Verdict on the bot
CLM6E4 is a **high-velocity new-launch rotation bot**: split-second funneled entries into fresh Pump.fun/stonkfun launches, tight stops on the spray, and a small conviction book that rides breakouts. It also trades **MANLET** itself (bought $48K / sold $31K), reinforcing the ANSEM-leg/stonkfun cluster.

---

## Part 5 — Tooling & method notes

- All data via **`gmgn-cli`** (GMGN OpenAPI): `token info/security/traders/holders`, `portfolio stats/holdings/activity/token-balance`.
- Pagination: `portfolio activity` caps at 20 rows/page; follow the `next` cursor.
- Metrics computed in Python over raw JSON: burst clustering (≤5s), size histogram, hold-time distribution, per-token P&L.

### Caveats / limitations
- Sample is a window (08-01..08-28), not full lifetime; ANSEM's +$121K occurred outside this exact buy sample but was captured in the per-token summary.
- `arbitrager`/terminal tags indicate automation; cannot see actual execution code — strategy is *inferred* from observable on-chain trade patterns.
- Rates limited: ~1 req/sec sustained for activity pages.

---

## Part 6 — Operations cost estimate (paper vs real trading)

Modeled on the CLM6E4 pattern reproduced in BUILD_PROMPT.md: **~24 swap txs/day, ~8 fresh-launch
entries/day, 3-slice funnel buys, median slice ~$1.3K**. Assumes SOL ≈ $101 and equal traffic in
paper and real mode. Paper runs the identical pipeline but never signs/lands on-chain txs.

### Paper trading (simulation)

| Item | $/mo |
|---|---|
| RPC + Geyser launch/mempool stream | 50 |
| Postgres (managed) | 25 |
| Compute / hosting | 30 |
| Observability + alerts | 15 |
| On-chain tx cost | **0** |
| **Paper total** | **~$120/mo** |

The real sunk cost of paper mode is the **one-time dev/build effort** (M0–M6 in BUILD_PROMPT.md),
not the monthly run rate. Monthly infra can be trimmed to ~$40–60 with a cheap VPS, free-tier DB,
and a single RPC.

### Real trading (adds transaction cost)

| Item | $/mo (median) | $/mo (aggressive) |
|---|---|---|
| Infra (same as paper) | 120 | 120 |
| Buy funnels — 243 Jito bundles | 124 (tip ~$0.51) | ~507 (tip $2.02) |
| Sell txs — ~496 @ priority fee | 26 | 26 |
| **Real total** | **~$270/mo** | **~$640/mo** |

### Caveats that dominate real costs

- **Slippage dwarfs fixed fees.** The bot touches roughly **~$960K/mo notional**; 1–5% slippage on
  fresh launches costs way more than the ~$270 fixed tx bill. The real "ops cost" is spread/price
  impact on ~$1K slices, not transaction fees.
- **Jito tip is the wildcard.** In a contested snipe window a single tip can reach 0.02–0.05 SOL
  (~$2–5). Missed bundles (opportunity cost) are not reflected here.
- **Mempool RPC tier** needed for the snipe edge may require 2–3 RPCs; the $50 line scales with
  capacity.
- Cost per trade must be netted against the strategy's negative-expectancy spray — the model relies
  on rare 100x outliers (e.g. ANSEM +$121K) to clear both fees and slippage.

---

*End of report.*
