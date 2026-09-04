# hfmcbot

SOL meme-coin **"new-launch spray + let winners run"** trading bot in Rust — a
production-oriented implementation of the strategy reverse-engineered from
wallet `CLM6E4…Kg1Q` (see `REPORT.md`), built to the spec in `BUILD_PROMPT.md`.

> **Risk notice:** real-money bot territory targeting fresh meme launches. EV
> is dominated by rare 100x outliers; the naive spray loses on most trades.
> Ship risk controls first, run paper mode, never skip the replay sign-off.
> Not financial advice, no profitability guarantee.

## Status — M0–M6 shipped (live boots simulate-only by default; no real funds yet)

| Milestone | State | Notes |
|---|---|---|
| M0 Skeleton (config, logging, audit, validation) | ✅ | `config.rs`, `persist.rs`, `keys.rs` (operator keypair, secret never logged), `metrics.rs` (`/metrics` + `/healthz`, heartbeat, panic hook) |
| M1 Ingest (Geyser/WS launch + price feed) | ✅ foundation + soak-verified | `ingest.rs` (`LaunchFeed` trait + `ReplayFeed` + Yellowstone `LiveFeed` mapping w/ slot clock) + `wsfeed.rs` (free pumpportal WS subscriber, backoff, graceful shutdown) + `decode.rs` (pump/stonkfun instruction decoders, live-fixture tested); `src/bin/record.rs` soak recorder; 216-create live soak replayed through paper (Step 2 sign-off: GO) |
| M2 Decision (entry gate, funnel sizing, stop/trail) + backtest harness | ✅ | `strategy.rs`, `engine.rs`, `ReplayFeed` |
| M2 Risk/circuit breakers (**in M2, not deferred**) | ✅ | `risk.rs` — kill switch, daily-loss breaker, throttle, position cap |
| M3 Execution (paper) | ✅ | `exec.rs` — async `Executor` trait + depth-aware `PaperExecutor`: fill slippage = base + impact coeff × (notional/liquidity), capped; failed exits count toward `HFM_MAX_EXIT_ATTEMPTS` then mark the position stuck (funds stay deployed, sweep skips it, fresh ticks retry) |
| M3.5 Live-exec scaffolding | ✅ | `live.rs` — Jupiter Swap API v1 quote client (URL builder, response parser, slippage-budget preflight) + Jito `sendBundle` builder + token-bucket governor; `LiveExecutor` refuses paper mode and enforces the funnel deadline |
| M4 devnet pipeline | ✅ | Full quote → preflight → `/swap` assembly (USD→lamport/raw-unit math) → local signing (payer-checked `VersionedTransaction`) → Jito bundle → landing reconciliation (`Landed`/`Failed`/`Pending`, timeout = unknown). `main.rs` live boot: keypair required, RPC blockhash self-check, replay refused in live, layered safety (config → armed → `HFM_SIMULATE_ONLY=false` to send, default true) |
| M5 mainnet-beta / M6 harden | ✅ (code; no funds risked) | Live loop (`liveloop.rs`: WS creates + per-token trade ticks → engine → executor, shadow mode for paper); crash-safe snapshots (`persist.rs` `EngineState`, atomic save, resume-on-boot, live refuses corrupt snapshots); multi-RPC failover (`HFM_RPC_FALLBACK_URLS`, sticky-success); Jito tip tiers (entry/flip/conviction → swap priority fee + standalone tip-transfer tx in-bundle + fill-fee accounting); alerting (`alerts.rs`: log always + rate-limited webhook for kill/breaker/stuck/unknown/snapshot-failure); balance reconciliation (landed fill must match wallet within `HFM_RECONCILE_DUST_TOKENS`, else Transport + page); blockhash manager (`HFM_BLOCKHASH_TTL_SECS` cache); Postgres audit mirror (opt-in `--features pg` + `HFM_PG_DSN`, JSONL stays primary); devnet proof binary (`src/bin/devnet_trade.rs`: throwaway key → faucet → self-send → confirm, devnet-guarded). 145/145 tests (149 with `--features pg`). Before real funds: the 8-gate checklist below |

## Quick start

```bash
cp .env.example .env          # tune thresholds; every threshold is env-driven
cargo run -- data/example_events.jsonl   # replay the bundled demo feed
cargo test                    # 145 tests (139 unit + 4 chaos + 2 replay; 149 with --features pg)
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
| Solana RPC | provider-dependent (e.g. ~10 req/s on free tiers) | `HFM_MAX_TRADES_PER_MIN` throttle + `HFM_RPC_FALLBACK_URLS` failover (M6: transport-errors-only, sticky-success, 15s per-attempt timeout) |
| Jupiter quote API | ~1 req/s sustained (free tier) | exact-out quotes cached per funnel; rate-limited by same governor |
| Geyser/WS stream | subscription caps per plan | one stream, event fan-out internal |
| GMGN (data, M1 aliasing) | ~1 req/s sustained (observed in REPORT.md Part 5) | alias resolution polls are throttled |

## Testing

- Unit: slippage/fee math, entry gate, slice splitting, TP/SL/time/trail
  transitions, breaker behavior, JSONL persistence.
- Integration (`tests/replay.rs`): the CLM6E4-pattern spray — 6 losers + 1
  outlier — must capture the winner in conviction mode, cut all losers, and
  finish net-positive (the spec's replay EV gate).

## Live boot (M4)

```bash
HFM_MODE=live HFM_SOL_USD=103.77 HFM_SECRET_KEY='<key>' \
  HFM_RPC_URL=https://api.devnet.solana.com ./target/debug/hfmcbot
```

Boot order: config validation (live requires `HFM_SOL_USD > 0`) → operator
keypair required → RPC self-check (fresh `getLatestBlockhash`, unreachable =
no boot) → state-snapshot resume if present (corrupt snapshot = no boot in
live, fresh start in paper) → `LiveExecutor::armed_with_signer` → replay files refused in live
mode. With the default `HFM_SIMULATE_ONLY=true` swaps are assembled + signed
but never submitted; set it `false` to send via Jito bundles. Only the pubkey
is ever logged — never the secret. Snapshots checkpoint every
`HFM_STATE_EVERY_SECS` (default 60) plus on shutdown/disconnect; alerts
(`HFM_ALERT_WEBHOOK_URL`, rate-limited by `HFM_ALERT_MIN_SECS`) page on kill,
breaker trips, stuck positions, unknown exits, and snapshot failures.

## Roadmap (M5/M6)

 1. ~~Live Geyser/WS `LaunchFeed` (pump.fun/stonkfun `create`/`trade`).~~ **Shipped + soak-verified (M1/Step 2)** — price-path feed (trades → `PriceUpdate` ticks, M5) drives flip exits live; exits also sweep on every event so a dead feed can't strand positions past max-hold.
 2. ~~Jupiter quote → swap tx builder; Jito bundle with configurable tip tier.~~ **Full pipeline shipped (M3.5/M4)** — quote → preflight → `/swap` → sign → bundle → reconcile; mock-upstream e2e + devnet blockhash check green. **Tiers shipped (M6)** — entry/flip/conviction priced via `HFM_JITO_TIP_*_LAMPORTS`, expressed on-chain as the swap priority fee AND as a standalone tip-transfer tx bundled swap-first/tip-second (`HFM_JITO_TIP_ACCOUNT`; sends fail-closed without one).
 3. ~~Postgres audit tables (replacing JSONL via the same `AuditRecord` schema).~~ **Mirror shipped (opt-in `--features pg` + `HFM_PG_DSN`)** — every JSONL record is also inserted into `hfmcbot_audit` (ts/kind/mint indexed, full record as JSONB) by a non-blocking background worker; JSONL stays the primary trail, the mirror is best-effort (overflow/insert failures drop + count + ERROR-log, never stall trading; live boot refuses on a dead mirror, paper warns). NoTls — run behind localhost/tunnel/proxy.
  4. ~~Alerting webhook; multi-RPC failover;~~ ~~Jito tip-transfer ix;~~ ~~blockhash manager;~~
     ~~balance-based reconciliation;~~
     devnet funds-moving e2e (binary shipped + mock-proven; first live run
     faucet-blocked 2026-09-04, retry pending) + replay sign-off gate before
     mainnet-beta.
    (Prometheus `/metrics` + `/healthz` already serve on `HFM_METRICS_ADDR`.)
    Balance reconciliation: after a landed bundle the wallet MUST show what the
    fill claims (buys cover qty, sells leave dust-or-empty within
    `HFM_RECONCILE_DUST_TOKENS`) or the fill is UNKNOWN (Transport) and the
    operator is paged. Blockhashes are cached for `HFM_BLOCKHASH_TTL_SECS`
    (default 30). Devnet proof: `cargo run --bin devnet_trade` (devnet-guarded,
    throwaway keypair, faucet → self-send → confirm).

## Pre-mainnet / real-funds checklist

Do these in order; each gates the next. Nothing here is optional.

 1. **Devnet e2e green** — `cargo run --bin devnet_trade` prints `DEVNET E2E
    OK` (proves sign → submit → confirm moves value on a real chain).
 2. **Long paper-live soak with ticks** — shadow mode
    (`HFM_SOAK_WS_URL` + `HFM_SOL_USD`, paper executor on live data) for
    multiple hours; require `ws_unparsed_* == 0`, reconnects clean, and
    `ws_trades > 0` on watched mints (proves the server honors trade subs).
 3. **Replay sign-off on tick data** — closing trades (not just opens) replay
    through the paper engine; stop style + slippage coeff calibrated here.
 4. **Fund a throwaway operator key** with the max-loss budget ONLY
    (equity + one day of tips/fees, nothing else on the key).
 5. **Mainnet simulate-only first** — `HFM_MODE=live` + `HFM_SIMULATE_ONLY=true`
    on mainnet: real quotes, real assembly, real signatures, zero submissions.
    Watch `/metrics` + alerts for a full session.
 6. **Kill-switch rehearsal** — flip `HFM_KILL_SWITCH=true` mid-session and
    confirm entries halt while exits proceed, then resume from the snapshot.
 7. **Small-size live** — `HFM_SIMULATE_ONLY=false` with minimum budgets;
    confirm landing + reconciliation + `hfmcbot_stuck_positions == 0`.
 8. **Scale up only after a green week** — then revisit sizing
    (`HFM_RISK_PER_TRADE_PCT`, `HFM_MAX_OPEN_POSITIONS`) with live data.
