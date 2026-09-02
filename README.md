# hfmcbot

SOL meme-coin **"new-launch spray + let winners run"** trading bot in Rust — a
production-oriented implementation of the strategy reverse-engineered from
wallet `CLM6E4…Kg1Q` (see `REPORT.md`), built to the spec in `BUILD_PROMPT.md`.

> **Risk notice:** real-money bot territory targeting fresh meme launches. EV
> is dominated by rare 100x outliers; the naive spray loses on most trades.
> Ship risk controls first, run paper mode, never skip the replay sign-off.
> Not financial advice, no profitability guarantee.

## Status — M0–M3 (paper) shipped

| Milestone | State | Notes |
|---|---|---|
| M0 Skeleton (config, logging, audit, validation) | ✅ | `config.rs`, `persist.rs` |
| M1 Ingest (Geyser/WS launch + price feed) | ⬜ stub | `ingest.rs` has the `LaunchFeed` trait + deterministic `ReplayFeed`; live WS feed plugs into the same trait |
| M2 Decision (entry gate, funnel sizing, stop/trail) + backtest harness | ✅ | `strategy.rs`, `engine.rs`, `ReplayFeed` |
| M2 Risk/circuit breakers (**in M2, not deferred**) | ✅ | `risk.rs` — kill switch, daily-loss breaker, throttle, position cap |
| M3 Execution (paper) | ✅ | `exec.rs` — `Executor` trait + slippage/fee-simulating `PaperExecutor`; Jupiter/Jito live impl lands in M3.5/M4 |
| M4 devnet / M5 mainnet-beta / M6 harden | ⬜ | config refuses `HFM_MODE=live` until then |

## Quick start

```bash
cp .env.example .env          # tune thresholds; every threshold is env-driven
cargo run -- data/example_events.jsonl   # replay the bundled demo feed
cargo test                    # 27 unit + integration tests
```

Replay a feed of your own: JSONL, one event per line (`{"type":"launch",...}`
or `{"type":"price",...}` — see `src/types.rs` for exact fields and
`data/example_events.jsonl` for a working sample).

## Architecture

```
[Ingest] → [Strategy/Decision] → [Risk] → [Execution] → [Persistence]
 ingest.rs   strategy.rs          risk.rs    exec.rs        persist.rs
             engine.rs (wiring: one state machine per token)
```

- **Entry gate** (all must pass): age ≤ `HFM_ENTRY_MAX_AGE_SECS`; on-curve OR
  graduated with liquidity ≥ `HFM_MIN_LIQUIDITY_USD`; creator hold ≤
  `HFM_MAX_DEV_HOLD_PCT`; not honeypot; mint renounced on migrated pools.
- **Funnel entry**: position split into `HFM_FUNNEL_SLICES` slices fired
  back-to-back (the ≤5s burst from the wallet study); partial funnels allowed
  if the throttle cuts in.
- **Sizing (risk-first)**: `min(risk_per_trade_pct × equity, max_single_pos_usd)`
  capped by free capital.
- **Exit**: flip mode (TP `+HFM_TAKE_PROFIT_PCT`, stop `HFM_STOP_LOSS_PCT`
  below high-water mark, time stop `HFM_MAX_HOLD_SECS`); promotion to
  conviction at `+HFM_CONVICTION_MIN_PCT` → trail `HFM_TRAIL_PCT` below the
  high-water mark, hold up to `HFM_CONVICTION_MAX_HOLD_DAYS`.
- **Risk controls**: kill switch (`HFM_KILL_SWITCH` + programmatic), daily-loss
  breaker (`HFM_DAILY_LOSS_LIMIT_PCT`), `HFM_MAX_TRADES_PER_MIN` throttle on
  all outbound orders, `HFM_MAX_OPEN_POSITIONS` cap. **Sells are never blocked**
  — cutting losers must always be possible.
- **Money math**: `rust_decimal` everywhere — no floating point for money.
- **Audit trail**: append-only JSONL (`HFM_AUDIT_LOG_PATH`) — every decision,
  order, fill, closed trade, breaker event, flushed per record.

## Config

Every trading threshold is env-driven — no hardcoded magic numbers. See
`.env.example` for the full annotated list (`HFM_*` prefix). Invalid configs
fail fast at boot (validation in `config.rs`).

## Upstream rate limits (documented per spec §9)

| Upstream | Limit | Handling |
|---|---|---|
| Solana RPC | provider-dependent (e.g. ~10 req/s on free tiers) | `HFM_MAX_TRADES_PER_MIN` throttle + failover list (M4) |
| Jupiter quote API | ~1 req/s sustained (free tier) | exact-out quotes cached per funnel; rate-limited by same governor |
| Geyser/WS stream | subscription caps per plan | one stream, event fan-out internal |
| GMGN (data, M1 aliasing) | ~1 req/s sustained (observed in REPORT.md Part 5) | alias resolution polls are throttled |

## Testing

- Unit: slippage/fee math, entry gate, slice splitting, TP/SL/time/trail
  transitions, breaker behavior, JSONL persistence.
- Integration (`tests/replay.rs`): the CLM6E4-pattern spray — 6 losers + 1
  outlier — must capture the winner in conviction mode, cut all losers, and
  finish net-positive (the spec's replay EV gate).

## Roadmap (M4+)

1. Live Geyser/WS `LaunchFeed` (pump.fun/stonkfun `create`/`trade`).
2. Jupiter quote → swap tx builder; Jito bundle with configurable tip tier.
3. Postgres audit tables (replacing JSONL via the same `AuditRecord` schema).
4. Prometheus `/metrics` + alerting webhook; multi-RPC failover; blockhash
   manager; devnet e2e; replay sign-off gate before mainnet-beta.
