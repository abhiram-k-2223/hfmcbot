# Build Prompt — SOL Meme "New-Launch Spray + Let Winners Run" Bot (Rust → Production)

This is a self-contained spec to build a production Solana meme-coin trading bot in Rust that
reproduces the strategy observed in wallet `CLM6E4…Kg1Q` (see REPORT.md).

> Strategy summary the bot must reproduce:
> - **Target:** brand-new launchpad tokens on Pump.fun and stonkfun (and any `ray`/`meteora_virtual_curve` DPAMs).
> - **Entry:** split-second funneled multi-buy (2–3 slices within ~5s) to minimize slippage and front-run the move.
> - **Sizing:** spread capital — median slice ~$1–5K per token; never all-in on one launch.
> - **Exit:** two modes — (a) flip: cut losers fast (~1h or on -X% stop), (b) conviction: let a few breakout winners run for days (up to 15d; trail the move).
> - **Reality:** net edge comes from rare 100x outliers (e.g. ANSEM +$121K). The bot must budget for a mostly-losing spray and a few huge wins.

---

## 1. Architecture (high level)

```
[Ingest] → [Strategy/Decision] → [Execution] → [Risk/Persistence]
   │              │                    │               │
 RPC + WS      filters/          Jito tip +        Postgres /
 Geyser        sizing             jupiter swap     files
```

- **Language:** Rust (edition 2021+, `tokio` async runtime).
- **Target:** Ubuntu production host (or Docker), variable-latency internet; aim < 1s end-to-end hot path.

## 2. Core dependencies

| Concern | Crate | Notes |
|---|---|---|
| Async runtime | `tokio` | `full` features |
| Solana RPC/program | `solana-client`, `solana-sdk` | latest stable |
| WebSocket | `tokio-tungstenite` | for Geyser/streams |
| Pump.fun / stonkfun program IDs | `pumpdotfun-sdk` (community) | verify IDs each release; or hand-roll CPI structs |
| DEX routing / quote | `jupiter-rs` or hand-rolled Jupiter API client | route swaps, exact-out |
| Jito MEV bundling | `jito-sdk` (community) or raw Jito RPC | for speed/front-run |
| Key management | `solana-sdk` local keypair + env | NEVER in git |
| DB | `sqlx` (Postgres) or `sled`/`redb` for embedded | store trades, state |
| Config | `config` / `clap` / `dotenvy` | env-driven |
| Logging | `tracing` + `tracing-subscriber`, honeycomb/jaeger optional | production observability |
| Slippage math | `decimal` / `rust_decimal` | NEVER float for money |

## 3. Data ingestion

1. **New-launch feed**
   - Subscribe to Pump.fun and stonkfun program `create` + `trade` events via **Geyser (gRPC)** or WebSocket streaming (e.g. with a provider like Helius/QuickNode).
   - Maintain an in-memory map: `token -> {mint, bump, vBondingCurve, pool, created_at, virtual_price, liquidity}`.
   - Alias resolution: also poll Jupiter token-list + GMGN-style market feed for newly-listed stonkfun tokens.
2. **Continuous book/price**
   - For each tracked token, subscribe to pool quote updates (Pump.fun bonding curve virtual SOL reserve; stonkfun AMM pool).
   - Compute live price = `quote_reserve/base_reserve` (with decimals handled via `rust_decimal`).
3. **Mempool (optional but recommended for front-run)**
   - Geyser mempool stream to see large pending buys; gives the "before everyone else" edge.

## 4. Strategy / decision module

Implement as a state machine per token. Thresholds are CONFIG-DRIVEN (env), not hardcoded.

**Entry gate (all must pass):**
- Age: `now - created_at <= entry_max_age` (e.g. ≤ 10 min) OR fresh launch detected in feed.
- **Liquidity litmus:** token is still on curve OR just graduated with real liquidity (`liquidity >= min_liq`, e.g. > $50k).
- **Creator/security guard:** skip if creator holds > `max_dev_hold` % OR mint not renounced on migrated pool OR `is_honeypot`.
- **Contract checks:** verify `data_len`/`is_initialized`, no malicious CPI targets.
- **Slippage budget:** predicted fill at `max_slippage` (e.g. 5–10%, configurable; higher on fresh launches).

**Funnel entry (reproduce the ≤5s burst):**
- Split intended size `S` into `N` slices (default N=3) and send them back-to-back within `<=5s`:
  - prefer Jito bundle (tip tier configurable) to land before competing snipers;
  - each slice exact-out with per-slice slippage so the batch doesn't reprice catastrophically.
- Optionally subscribe to "same wallet re-buys" heuristic (the observed pattern re-buys 2–3x on the same token).

**Exit rules (two modes):**
- **Stop mode (default for the spray):** 
  - `take_profit` point e.g. +80–150% → sell all (configurable);
  - `stop_loss` e.g. -30% from last higher close → sell all;
  - `max_hold_seconds` (e.g. ≤ 6h) → close if not in conviction.
- **Conviction/trail mode (promote on strong breakout):**
  - if token ≥ `conviction_min` (e.g. +300%) and volume/price structure intact → trail a stop (`trail_pct`, e.g. 25%) instead of hard TP;
  - allow multi-day holds (cap `conviction_max_hold`).

**Position sizing (risk-first):**
- Per-token max = `risk_per_trade_pct * equity` with a hard `max_single_pos_usd`.
- Global: cap total concurrent open positions and per-launchpad exposure.
- Must survive a mostly-losing spray: model expected value as `few big wins >> many small losses`.

## 5. Execution module (hot path)

1. **Signing/keys:** keypair from `SECRET_KEY` env (serde/base58); store encrypted at rest.
2. **RPC selection:** primary + failover endpoints; health-check; keep local current blockhash.
3. **Jupiter routing:** request quote → build swap tx; prefer exact-out; fallback to direct pool CPI if seed-quote unavailable.
4. **Jito bundling:** wrap the funnel slices (and optionally a counter-tx for front-run) into a bundle; tip via `ComputeBudget`.
5. **Landing confirmation:** poll signature with `getSignatureStatuses`; reconcile on timeout (fetch state, don't blind-resubmit).
6. **Priority fees:** dynamic fee based on current min-context window / fee market (rate-limited).

## 6. Risk & controls (NON-NEGOTIABLE for "to prod")

- **Circuit breakers:** kill switch (env flag or remote), daily-loss limit (e.g. -10%), max trades/min throttling.
- **Rate limiting:** outbound RPC + Jupiter with token-bucket governor to avoid 429/bans.
- **Nonce/blockhash management:** refresh periodically; never reuse.
- **Idempotency:** each order has an id; reconcile against on-chain state before re-issuing.
- **Health/alerting:** heartbeat + `tracing` error/panic hooks → Slack/Telegram webhook; Prometheus metrics (`/metrics`).
- **Audit trail:** every decision + order + fill persisted to Postgres (or `redb` for embed); immutable trades table.
- **Secrets:** `.env` via `dotenvy`, secret key excluded from build cache; no secrets in logs (redact tx private keys).

## 7. Testing strategy

- **Unit:** slippage math, state machines, sizing, stop/trail logic, fee calc (property tests on `rust_decimal`).
- **Integration (devnet):** deploy a fake "Pump.fun" dev program → run full funnel+exit, assert tx landed.
- **Replay/backtest:** replay historical launch feed through the decision module (the CLM6E4 pattern data) — assert it *would* have taken ANSEM and cut the rest. Gate on: positive net EV over the replay set AND max-drawdown < cap.
- **Chaos:** kill RPC, force 429, insert fee spikes — assert circuit breakers trip, no stuck funds.

## 8. Milestones / delivery order

1. **M0 — Skeleton:** config, logging, DB schema, key mgmt, health endpoint.
2. **M1 — Ingest:** Geyser/WS launch + price feed; store tokens; unit tests.
3. **M2 — Decision:** entry gate + funnel sizing + exit/trail; backtest harness on collected data.
4. **M3 — Execution (paper):** route + sign + simulate; no real funds; assert tx composition.
5. **M4 — Devnet live:** end-to-end on devnet against a mock launch.
6. **M5 — Mainnet-beta:** small capital, kill switch, daily-loss limit, full observability.
7. **M6 — Prod harden:** trailing, multi-RPC failover, Jito priority tiers, audit/alerting, replay-sign-off.

## 9. Explicit constraints / acceptance criteria

- Hot path (launch event → signed tx) target **< 1s** (Jito bundle landing considered success).
- **No floating point for money** — `rust_decimal` only.
- **Never** log or print the private key, keypair seed phrase, or signed tx data.
- All trading thresholds configurable via env — **no hardcoded magic numbers** in code.
- Must include the circuit breakers in M2, not deferred.
- Document rate limits for every upstream (GMGN/Jupiter/Geyser/RPC).

---

> **Risk notice:** This is a real-money trading bot targeting fresh meme launches. Expected value is
> dominated by rare multi-hundred-x outliers and the naive "spray" loses on most individual trades.
> Ship the risk/controls from M0 and start on devnet/paper. This spec reproduces an observed
> on-chain pattern — it is not financial advice and there is no guarantee of profitability.
