//! Free-tier WebSocket feed: pumpportal `subscribeNewToken` (D13).
//!
//! Why this exists: no free Yellowstone gRPC tier streams enough for a soak,
//! so the soak runs on pumpportal's keyless WebSocket while paid gRPC stays
//! the paper-live/production path (`LiveFeed` in [`crate::ingest`]). The two
//! paths converge on the same types: WS creates become [`RawFeedEvent`]s with
//! `slot: None` (the WS sends no slot — receive wall-clock is the timestamp)
//! and the same raw/event JSONL archive the recorder writes.
//!
//! Wire-shape honesty: the exact pumpportal JSON field names were captured
//! once from a live stream, not from a schema. [`extract_create`] therefore
//! accepts alias lists for every field, and — critically — the recorder
//! archives the ORIGINAL message line verbatim whatever happens. A parse miss
//! loses nothing but time: [`FeedStats::ws_unparsed_create`] counts wire
//! messages that carried a `mint` yet didn't decode, which is the alarm that
//! our aliases went stale. Valid-JSON-without-`mint` lines are protocol
//! chatter (subscribe acks, heartbeats), counted separately from malformed
//! (not-JSON) lines so chatter is never mistaken for data loss.
//!
//! Enrichment honesty ([`enrich_create`]): a WS create carries curve reserves
//! (`vSol`/`vTokens`) and the creator's `initialBuy`, but no USD price and no
//! holder snapshot. Engine-ready `Launch` events therefore need an explicit
//! `sol_usd` rate (recorder flag, logged per run — stale-rate risk is on the
//! operator, not hidden) and every derived number documents its assumption:
//! * price/liquidity come straight from reported reserves (`vSol` float SOL ÷
//!   `vTokens` float whole tokens — calibrated live, see
//!   [`PumpPortalCreate`]);
//! * creator share is `initialBuy` tokens ÷ 1e9-token supply DIRECTLY (the
//!   wire reports tokens received, verified against reserve deltas — no
//!   invariant math, no fee bound needed). `initialBuy == 0` is exactly 0%;
//! * `mint_renounced=false`, `is_honeypot=false` are UNVETTED placeholders —
//!   the entry gate only checks renounce on migrated pools and WS launches
//!   are always `on_curve`, but sign-off must treat these as unvetted.
//!
//! Anything that violates an assumption returns `None` (fail closed): the raw
//! line stays archived, no engine event is emitted, and the skip is logged.

use crate::ingest::{FeedStats, RawFeedEvent, RawKind, StampedRawEvent};
use crate::types::{Event, Launch, Launchpad, PriceUpdate};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default keyless stream (D13 soak source).
pub const PUMPPORTAL_WS_URL: &str = "wss://pumpportal.fun/api/data";

/// Message sent right after connect to subscribe to new-token creates.
pub const SUBSCRIBE_NEW_TOKEN: &str = r#"{"method":"subscribeNewToken"}"#;

/// Connection settings. No secrets involved (keyless stream), plain Debug.
#[derive(Debug, Clone)]
pub struct WsFeedConfig {
    pub url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum WsConfigError {
    #[error("bad WS url '{0}' (expected ws:// or wss://)")]
    BadUrl(String),
}

impl WsFeedConfig {
    pub fn from_parts(url: &str) -> Result<WsFeedConfig, WsConfigError> {
        let url = url.trim();
        if url.starts_with("ws://") || url.starts_with("wss://") {
            Ok(WsFeedConfig { url: url.into() })
        } else {
            Err(WsConfigError::BadUrl(url.into()))
        }
    }
}

/// Classification of one inbound WS text line.
#[derive(Debug)]
pub enum WsInbound {
    /// Carries a `mint` field — a candidate create, still to be extracted.
    CreateAttempt(serde_json::Value),
    /// `mint` + `txType` buy/sell — a candidate trade tick (M5 price path).
    TradeAttempt(serde_json::Value),
    /// Valid JSON without a `mint` (acks, heartbeats): not data loss.
    Chatter,
    /// Not JSON at all.
    Malformed,
}

/// Parse one inbound text line. Never panics, never loses data — the caller
/// archives the original line regardless of the outcome. Side is read from
/// `txType`: buy/sell lines route to the trade path; "create" or untyped
/// mint lines stay on the create path. A mint line with an UNKNOWN txType
/// also takes the create path — where `extract_create` rejects it into the
/// `ws_unparsed_create` alarm (fail loud, never silently swallowed).
pub fn parse_ws_line(line: &str) -> WsInbound {
    let v: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return WsInbound::Malformed,
    };
    match v.get("mint").and_then(|m| m.as_str()) {
        Some(m) if !m.is_empty() => match v.get("txType").and_then(|t| t.as_str()) {
            Some("buy") | Some("sell") => WsInbound::TradeAttempt(v),
            _ => WsInbound::CreateAttempt(v),
        },
        _ => WsInbound::Chatter,
    }
}

/// A pumpportal new-token create with every field we consume.
///
/// UNITS (calibrated against live wire data 2026-09-03, NOT assumed):
/// `vSolInBondingCurve` is float SOL, `vTokensInBondingCurve` float whole
/// tokens, `initialBuy` float whole TOKENS received by the creator (verified:
/// `1_073_000_000 − vTokens == initialBuy` on live creates, and `solAmount`
/// satisfies the constant-product check `dy = T·dx/(S+dx)`). All kept as f64
/// — magnitudes (~1e9) are exactly representable enough for USD pricing and
/// hold-% (relative precision ~1e-15, far below fee noise).
#[derive(Debug, Clone)]
pub struct PumpPortalCreate {
    pub mint: String,
    pub signature: String,
    pub trader: String,
    pub bonding_curve: String,
    pub vtokens_tokens: f64,
    pub vsol_sol: f64,
    pub initial_buy_tokens: f64,
    pub sol_amount: f64,
    pub market_cap_sol: f64,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub pool: String,
    pub mayhem: bool,
}

/// Accept an integer, float, or numeric string (wire shapes vary).
fn num_flex(v: &serde_json::Value) -> Option<f64> {
    if let Some(n) = v.as_u64() {
        return Some(n as f64);
    }
    if let Some(n) = v.as_i64() {
        return Some(n as f64);
    }
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        return s.trim().parse::<f64>().ok();
    }
    None
}

fn first_num(obj: &serde_json::Map<String, serde_json::Value>, aliases: &[&str]) -> Option<f64> {
    aliases
        .iter()
        .filter_map(|a| obj.get(*a))
        .filter_map(num_flex)
        .next()
}

fn first_str(obj: &serde_json::Map<String, serde_json::Value>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .filter_map(|a| obj.get(*a))
        .filter_map(|v| v.as_str())
        .next()
        .map(|s| s.to_string())
}

fn bool_flex(v: &serde_json::Value) -> Option<bool> {
    v.as_bool().or_else(|| v.as_u64().map(|n| n != 0))
}

/// Extract a create from a `CreateAttempt` value. `None` = wire had a `mint`
/// but the fields didn't match our aliases — caller counts
/// `ws_unparsed_create` (stale-alias alarm) and keeps the raw line.
pub fn extract_create(v: &serde_json::Value) -> Option<PumpPortalCreate> {
    let obj = v.as_object()?;
    let mint = first_str(obj, &["mint"])?;
    if mint.is_empty() {
        return None;
    }
    // When the wire bothers to say what it is, believe it: anything that is
    // not a create on a create-only subscription is a protocol surprise worth
    // alarming on (unparsed counter), not a launch.
    if let Some(tx) = obj.get("txType").and_then(|t| t.as_str()) {
        if tx != "create" {
            return None;
        }
    }
    let vtokens = first_num(obj, &["vTokensInBondingCurve", "vTokens", "v_tokens"])?;
    let vsol = first_num(obj, &["vSolInBondingCurve", "vSol", "v_sol"])?;
    if !vtokens.is_finite() || !vsol.is_finite() || vtokens <= 0.0 || vsol <= 0.0 {
        return None;
    }
    let initial_buy = first_num(obj, &["initialBuy", "initial_buy"]).unwrap_or(0.0);
    if !initial_buy.is_finite() || initial_buy < 0.0 {
        return None;
    }
    Some(PumpPortalCreate {
        mint,
        signature: first_str(obj, &["signature", "txSignature"]).unwrap_or_default(),
        trader: first_str(obj, &["traderPublicKey", "creator", "trader"]).unwrap_or_default(),
        bonding_curve: first_str(obj, &["bondingCurveKey", "bonding_curve", "bondingCurve"])
            .unwrap_or_default(),
        vtokens_tokens: vtokens,
        vsol_sol: vsol,
        initial_buy_tokens: initial_buy,
        sol_amount: first_num(obj, &["solAmount", "sol_amount"]).unwrap_or(0.0),
        market_cap_sol: first_num(obj, &["marketCapSol", "market_cap_sol"]).unwrap_or(0.0),
        name: first_str(obj, &["name"]).unwrap_or_default(),
        symbol: first_str(obj, &["symbol"]).unwrap_or_default(),
        uri: first_str(obj, &["uri"]).unwrap_or_default(),
        pool: first_str(obj, &["pool"]).unwrap_or_default(),
        mayhem: obj
            .get("is_mayhem_mode")
            .or_else(|| obj.get("mayhem_mode"))
            .or_else(|| obj.get("mayhem"))
            .and_then(bool_flex)
            .unwrap_or(false),
    })
}

/// Pump supply: 1e9 whole tokens (denominator for creator share).
const PUMP_SUPPLY_TOKENS: Decimal = Decimal::from_parts(1_000_000_000, 0, 0, false, 0);

/// An enriched launch plus its derivation provenance (kept beside the engine
/// event in logs; the raw line stays the ground truth in the archive).
#[derive(Debug, Clone)]
pub struct EnrichedLaunch {
    pub event: Event,
    pub creator_hold_pct: Decimal,
    pub price_sol_per_token: Decimal,
    pub sol_usd_used: Decimal,
}

/// Derive an engine-ready `Launch` from a WS create. Returns `None` (fail
/// closed) when anything is off: non-positive/non-finite rate or reserves,
/// negative creator buy, or a creator share outside [0, 100].
pub fn enrich_create(
    c: &PumpPortalCreate,
    at: DateTime<Utc>,
    sol_usd: Decimal,
) -> Option<EnrichedLaunch> {
    if sol_usd <= Decimal::ZERO {
        return None;
    }
    // `from_f64_retain` is None on NaN/±inf — non-finite wire values fail
    // closed here, not downstream.
    let vsol = Decimal::from_f64_retain(c.vsol_sol)?;
    let vtok = Decimal::from_f64_retain(c.vtokens_tokens)?;
    if vsol <= Decimal::ZERO || vtok <= Decimal::ZERO {
        return None;
    }
    let price_sol = vsol / vtok;
    let price_usd = price_sol * sol_usd;
    let liquidity_usd = Decimal::from(2) * vsol * sol_usd;

    // The wire reports the creator's received TOKENS, so the share is direct:
    // buy ÷ 1e9-token pump supply × 100. No invariant, no fee bound.
    let buy = Decimal::from_f64_retain(c.initial_buy_tokens)?;
    if buy < Decimal::ZERO {
        return None;
    }
    let hold_pct = buy / PUMP_SUPPLY_TOKENS * Decimal::from(100);
    if hold_pct > Decimal::from(100) {
        return None;
    }

    Some(EnrichedLaunch {
        event: Event::Launch(Launch {
            mint: c.mint.clone(),
            launchpad: Launchpad::PumpFun,
            created_at: at,
            creator_hold_pct: hold_pct,
            mint_renounced: false, // UNVETTED placeholder (see module docs)
            is_honeypot: false,    // UNVETTED placeholder (see module docs)
            liquidity_usd,
            on_curve: true,
            price_usd,
        }),
        creator_hold_pct: hold_pct,
        price_sol_per_token: price_sol,
        sol_usd_used: sol_usd,
    })
}

/// Map a create to the shared raw-event type (`slot: None` — the WS sends no
/// slot). The trader key is hex-encoded to match the gRPC path's
/// `creator_hex` whenever it base58-decodes; otherwise stored as-is (the raw
/// archive always holds the verbatim original).
pub fn raw_of_create(c: &PumpPortalCreate) -> RawFeedEvent {
    let creator_hex = bs58::decode(c.trader.as_bytes())
        .into_vec()
        .map(|b| b.iter().map(|x| format!("{x:02x}")).collect::<String>())
        .unwrap_or_else(|_| c.trader.clone());
    RawFeedEvent {
        slot: None,
        mint: c.mint.clone(),
        kind: RawKind::Create {
            bonding_curve: c.bonding_curve.clone(),
            creator_hex,
            name: c.name.clone(),
            symbol: c.symbol.clone(),
            mayhem: c.mayhem,
        },
    }
}

/// A pumpportal per-token trade tick (M5 price-path feed,
/// `subscribeTokenTrade`). Same reserve fields as creates (`txType` buy/sell
/// instead of create) — price is derived the identical way, so a trade tick
/// and a create at the same reserves produce the same price (tested).
#[derive(Debug, Clone)]
pub struct PumpPortalTrade {
    pub mint: String,
    pub signature: String,
    pub trader: String,
    pub bonding_curve: String,
    pub buy: bool,
    pub vtokens_tokens: f64,
    pub vsol_sol: f64,
    pub sol_amount: f64,
    pub market_cap_sol: f64,
}

/// Extract a trade from a `TradeAttempt` value. `None` = side missing (not a
/// trade at all — caller misrouted) or reserves unusable; caller counts
/// `ws_unparsed_trade` and keeps the raw line.
pub fn extract_trade(v: &serde_json::Value) -> Option<PumpPortalTrade> {
    let obj = v.as_object()?;
    let buy = match obj.get("txType").and_then(|t| t.as_str()) {
        Some("buy") => true,
        Some("sell") => false,
        _ => return None,
    };
    let mint = first_str(obj, &["mint"])?;
    if mint.is_empty() {
        return None;
    }
    let vtokens = first_num(obj, &["vTokensInBondingCurve", "vTokens", "v_tokens"])?;
    let vsol = first_num(obj, &["vSolInBondingCurve", "vSol", "v_sol"])?;
    if !vtokens.is_finite() || !vsol.is_finite() || vtokens <= 0.0 || vsol <= 0.0 {
        return None;
    }
    Some(PumpPortalTrade {
        mint,
        signature: first_str(obj, &["signature", "txSignature"]).unwrap_or_default(),
        trader: first_str(obj, &["traderPublicKey", "creator", "trader"]).unwrap_or_default(),
        bonding_curve: first_str(obj, &["bondingCurveKey", "bonding_curve", "bondingCurve"])
            .unwrap_or_default(),
        buy,
        vtokens_tokens: vtokens,
        vsol_sol: vsol,
        sol_amount: first_num(obj, &["solAmount", "sol_amount"]).unwrap_or(0.0),
        market_cap_sol: first_num(obj, &["marketCapSol", "market_cap_sol"]).unwrap_or(0.0),
    })
}

/// Derive an engine-ready `Event::Price` from a trade tick. Same reserve math
/// as [`enrich_create`] (price = vSol/vTokens × rate, liquidity = 2·vSol ×
/// rate) — one formula for both paths, so creates and ticks agree. Fail-closed
/// on bad rate or reserves, same as creates.
pub fn enrich_trade(t: &PumpPortalTrade, at: DateTime<Utc>, sol_usd: Decimal) -> Option<Event> {
    if sol_usd <= Decimal::ZERO {
        return None;
    }
    let vsol = Decimal::from_f64_retain(t.vsol_sol)?;
    let vtok = Decimal::from_f64_retain(t.vtokens_tokens)?;
    if vsol <= Decimal::ZERO || vtok <= Decimal::ZERO {
        return None;
    }
    Some(Event::Price(PriceUpdate {
        mint: t.mint.clone(),
        ts: at,
        price_usd: vsol / vtok * sol_usd,
        liquidity_usd: Decimal::from(2) * vsol * sol_usd,
    }))
}

/// Map a trade to the shared raw-event type (`slot: None`, WS sends no slot).
/// Uses the honest [`RawKind::WsTrade`] variant — only side + curve id are
/// typed; everything else lives in the verbatim raw archive.
pub fn raw_of_trade(t: &PumpPortalTrade) -> RawFeedEvent {
    RawFeedEvent {
        slot: None,
        mint: t.mint.clone(),
        kind: RawKind::WsTrade {
            bonding_curve: t.bonding_curve.clone(),
            buy: t.buy,
        },
    }
}

/// pumpportal per-token trade subscription messages (M5). `keys` is the exact
/// mint set to watch; an empty `keys` unsubscribes everything server-side —
/// the loop therefore only sends unsubscribe with a non-empty list and
/// re-subscribes the surviving set after a reconnect.
pub fn subscribe_token_trade_msg(mints: &[String]) -> String {
    serde_json::json!({"method": "subscribeTokenTrade", "keys": mints}).to_string()
}

/// See [`subscribe_token_trade_msg`].
pub fn unsubscribe_token_trade_msg(mints: &[String]) -> String {
    serde_json::json!({"method": "unsubscribeTokenTrade", "keys": mints}).to_string()
}

/// Pure subscription reconciliation: given the currently OPEN mints (sorted,
/// from [`crate::engine::Engine::open_mints`]) and the currently SUBSCRIBED
/// set, decide what to (un)subscribe. Rules, in order:
///
///   1. Unsubscribes are exact — every subscribed mint that is no longer open
///      is dropped (no stale subs burning the fair-use budget, D13).
///   2. Surviving subs keep their slots — no churn on the steady state.
///   3. New subs fill remaining slots up to `cap`; excess new mints are DROPPED
///      (never evict a held token to make room — an unsubscribed open position
///      simply exits on its time stop instead of its price stop, which is the
///      safe degradation, not a missed exit).
///   4. Both outputs sorted — stable wire order, deterministic tests.
///
/// Returns `(to_subscribe, to_unsubscribe)`.
pub fn plan_trade_subs(
    open: &[String],
    subscribed: &HashSet<String>,
    cap: usize,
) -> (Vec<String>, Vec<String>) {
    let mut to_unsub: Vec<String> = subscribed
        .iter()
        .filter(|m| !open.contains(m))
        .cloned()
        .collect();
    to_unsub.sort();
    let mut to_sub = Vec::new();
    let mut used = subscribed.len().saturating_sub(to_unsub.len());
    for m in open {
        if used >= cap {
            break;
        }
        if !subscribed.contains(m) {
            to_sub.push(m.clone());
            used += 1;
        }
    }
    (to_sub, to_unsub)
}

/// Pure backoff step: double, capped at 60s. Tested without any network.
pub fn next_backoff_secs(current_secs: u64) -> u64 {
    (current_secs.saturating_mul(2)).min(60)
}

/// Soak-runner entry point: streams the WS until SIGINT/SIGTERM, archiving
/// every raw line verbatim plus (when `sol_usd` is set) engine-ready events.
/// Shutdown is graceful — the in-flight message finishes archiving before we
/// exit — so a restart loses nothing the raw archive can't re-derive. Only
/// called by the recorder binary — `main.rs` never calls it.
pub async fn run_ws_loop(
    cfg: WsFeedConfig,
    raw_path: std::path::PathBuf,
    events_path: std::path::PathBuf,
    sol_usd: Option<Decimal>,
    stats: Arc<Mutex<FeedStats>>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    // Shutdown broadcast: SIGINT (ctrl-C) or SIGTERM (service stop, timeout,
    // deploy) ends the run after the current message — see the `select!`s.
    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        #[cfg(unix)]
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("sigterm handler installs");
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        let _ = sd_tx.send(true);
    });
    let mut sd_rx = sd_rx;

    if let Some(rate) = sol_usd {
        tracing::info!(sol_usd = %rate, "WS enrichment enabled (operator-supplied rate)");
    } else {
        tracing::info!("WS enrichment disabled (no HFM_SOL_USD): raw-only archive mode");
    }
    let mut backoff_secs = 1u64;
    let mut last_msg: Option<Instant> = None;
    loop {
        if *sd_rx.borrow_and_update() {
            break; // signal arrived between iterations
        }
        {
            let mut st = stats.lock().unwrap();
            st.reconnects += 1;
        }
        tracing::warn!(url = %cfg.url, "ws feed connecting");
        match tokio_tungstenite::connect_async(cfg.url.as_str()).await {
            Ok((mut ws, _)) => {
                backoff_secs = 1;
                if let Err(e) = ws.send(Message::Text(SUBSCRIBE_NEW_TOKEN.into())).await {
                    tracing::warn!(error = %e, "subscribe send failed, reconnecting");
                } else {
                    tracing::info!("ws feed streaming (subscribed new-token)");
                    loop {
                        // Shutdown takes priority: finish the in-flight
                        // message first (it already ran), then exit — never
                        // abandon a message between raw-append and event-write.
                        let msg = tokio::select! {
                            _ = sd_rx.changed() => {
                                final_stats(&stats);
                                tracing::info!("shutdown during stream: exiting clean");
                                return;
                            }
                            m = ws.next() => m,
                        };
                        let Some(msg) = msg else { break }; // peer closed stream
                        let now_wall = Utc::now().timestamp_millis();
                        let now_mono = Instant::now();
                        let gap_secs = last_msg
                            .map(|t| now_mono.saturating_duration_since(t).as_secs())
                            .unwrap_or(0);
                        last_msg = Some(now_mono);
                        {
                            let mut st = stats.lock().unwrap();
                            if gap_secs > st.max_gap_secs {
                                st.max_gap_secs = gap_secs;
                            }
                        }
                        match msg {
                            Ok(Message::Text(line)) => {
                                let line = line.as_str();
                                // Archive FIRST: verbatim ground truth, whatever follows.
                                append_line(&raw_path, line);
                                let mut st = stats.lock().unwrap();
                                st.ws_messages += 1;
                                match parse_ws_line(line) {
                                    WsInbound::Malformed => st.ws_malformed += 1,
                                    WsInbound::Chatter => st.ws_chatter += 1,
                                    // Trade ticks on this loop: unreachable on
                                    // the create-only subscription, but handled
                                    // (not alarmed) so a future combined sub
                                    // just works — raw archived above, typed
                                    // raw + enriched Price recorded below.
                                    WsInbound::TradeAttempt(v) => match extract_trade(&v) {
                                        None => st.ws_unparsed_trade += 1,
                                        Some(t) => {
                                            st.ws_trades += 1;
                                            let stamped = StampedRawEvent {
                                                at_ms: now_wall,
                                                event: raw_of_trade(&t),
                                            };
                                            if let Err(e) =
                                                crate::ingest::record_raw(&events_path, &stamped)
                                            {
                                                tracing::warn!(error = %e, "record_raw failed");
                                            }
                                            if let Some(rate) = sol_usd {
                                                let at = DateTime::from_timestamp_millis(now_wall)
                                                    .unwrap_or_else(Utc::now);
                                                match enrich_trade(&t, at, rate) {
                                                    None => tracing::warn!(
                                                        mint = %t.mint,
                                                        "trade enrichment skipped"
                                                    ),
                                                    Some(ev) => {
                                                        if let Err(e) = crate::ingest::record_event(
                                                            &events_path,
                                                            &ev,
                                                        ) {
                                                            tracing::warn!(error = %e, "record_event failed");
                                                        }
                                                    }
                                                }
                                            }
                                            // Release the lock before any
                                            // further work (counters only).
                                            drop(st);
                                        }
                                    },
                                    WsInbound::CreateAttempt(v) => match extract_create(&v) {
                                        None => st.ws_unparsed_create += 1,
                                        Some(c) => {
                                            st.creates += 1;
                                            tracing::info!(
                                                mint = %c.mint,
                                                symbol = %c.symbol,
                                                initial_buy_tokens = %c.initial_buy_tokens,
                                                "ws create"
                                            );
                                            let stamped = StampedRawEvent {
                                                at_ms: now_wall,
                                                event: raw_of_create(&c),
                                            };
                                            if let Err(e) =
                                                crate::ingest::record_raw(&events_path, &stamped)
                                            {
                                                tracing::warn!(error = %e, "record_raw failed");
                                            }
                                            if let Some(rate) = sol_usd {
                                                let at = DateTime::from_timestamp_millis(now_wall)
                                                    .unwrap_or_else(Utc::now);
                                                match enrich_create(&c, at, rate) {
                                                    None => tracing::warn!(
                                                        mint = %c.mint,
                                                        "enrichment skipped (assumption violated)"
                                                    ),
                                                    Some(en) => {
                                                        if let Err(e) = crate::ingest::record_event(
                                                            &events_path,
                                                            &en.event,
                                                        ) {
                                                            tracing::warn!(error = %e, "record_event failed");
                                                        } else {
                                                            tracing::info!(
                                                                mint = %c.mint,
                                                                hold_pct = %en.creator_hold_pct,
                                                                price_usd = %match &en.event {
                                                                    Event::Launch(l) => l.price_usd,
                                                                    _ => Decimal::ZERO,
                                                                },
                                                                "recorded launch"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                            Ok(Message::Binary(_)) => {
                                let mut st = stats.lock().unwrap();
                                st.ws_messages += 1;
                                st.ws_chatter += 1; // unexpected frame, counted not crashed
                            }
                            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {
                            }
                            Ok(Message::Close(_)) => {
                                tracing::warn!("ws closed by peer, reconnecting");
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "ws error, reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, backoff_secs, "ws connect failed, retrying");
            }
        }
        // Backoff sleep that a shutdown signal cuts short.
        tokio::select! {
            _ = sd_rx.changed() => break,
            _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
        }
        backoff_secs = next_backoff_secs(backoff_secs);
    }
    final_stats(&stats);
    tracing::info!("ws feed stopped");
}

/// Log the cumulative counters — called on every exit path so a stopped soak
/// always reports its own scoreboard.
fn final_stats(stats: &Arc<Mutex<FeedStats>>) {
    let s = stats.lock().unwrap().clone();
    tracing::info!(
        ws_messages = s.ws_messages,
        creates = s.creates,
        malformed = s.ws_malformed,
        chatter = s.ws_chatter,
        unparsed_create = s.ws_unparsed_create,
        max_gap_secs = s.max_gap_secs,
        reconnects = s.reconnects,
        "soak stats"
    );
}

/// Append one verbatim line (plus newline) to the raw archive.
fn append_line(path: &Path, line: &str) {
    use std::io::Write;
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut f) => {
            if let Err(e) = f
                .write_all(line.as_bytes())
                .and_then(|_| f.write_all(b"\n"))
            {
                tracing::warn!(error = %e, "raw archive write failed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "raw archive open failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Real wire shape, captured live 2026-09-03 (float SOL / float tokens /
    /// float-token initialBuy — NOT lamports/base units).
    fn long_shape() -> serde_json::Value {
        serde_json::json!({
            "signature": "sig1",
            "mint": "MINT1111111111111111111111111111111111111",
            "traderPublicKey": "TRADER111111111111111111111111111111111",
            "txType": "create",
            "initialBuy": 3529685.84306,
            "solAmount": 0.099012169,
            "bondingCurveKey": "CURVE1111111111111111111111111111111111",
            "vTokensInBondingCurve": 1069470314.15694,
            "vSolInBondingCurve": 30.09901216881861,
            "marketCapSol": 28.143850063333048,
            "name": "BEN",
            "symbol": "BEN",
            "uri": "https://example.invalid/b.json",
            "pool": "pump",
            "is_mayhem_mode": true
        })
    }

    /// Same event in the short-alias shape.
    fn short_shape() -> serde_json::Value {
        serde_json::json!({
            "mint": "MINT2222222222222222222222222222222222222",
            "vTokens": 1052501650.328147,
            "vSol": 30.584275083999973
        })
    }

    #[test]
    fn config_accepts_only_ws_urls() {
        assert!(WsFeedConfig::from_parts("wss://x.invalid/feed").is_ok());
        assert!(WsFeedConfig::from_parts("ws://localhost:9").is_ok());
        assert!(WsFeedConfig::from_parts("https://x.invalid").is_err());
        assert!(WsFeedConfig::from_parts("").is_err());
    }

    #[test]
    fn parse_classifies_lines() {
        assert!(matches!(
            parse_ws_line(&long_shape().to_string()),
            WsInbound::CreateAttempt(_)
        ));
        assert!(matches!(
            parse_ws_line(r#"{"message":"subscribed"}"#),
            WsInbound::Chatter
        ));
        assert!(matches!(
            parse_ws_line(r#"{"mint":""}"#),
            WsInbound::Chatter
        ));
        assert!(matches!(parse_ws_line("not json{{{"), WsInbound::Malformed));
        assert!(matches!(parse_ws_line("[1,2]"), WsInbound::Chatter));
    }

    #[test]
    fn extract_accepts_both_alias_shapes() {
        let c = extract_create(&long_shape()).unwrap();
        assert_eq!(c.mint, "MINT1111111111111111111111111111111111111");
        assert_eq!(c.bonding_curve, "CURVE1111111111111111111111111111111111");
        assert!((c.vtokens_tokens - 1069470314.15694).abs() < 1e-6);
        assert!((c.vsol_sol - 30.09901216881861).abs() < 1e-9);
        assert!((c.initial_buy_tokens - 3529685.84306).abs() < 1e-6);
        assert_eq!(c.symbol, "BEN");
        assert!(c.mayhem);

        let c2 = extract_create(&short_shape()).unwrap();
        assert_eq!(c2.mint, "MINT2222222222222222222222222222222222222");
        assert!((c2.vsol_sol - 30.584275083999973).abs() < 1e-9);
        assert_eq!(c2.initial_buy_tokens, 0.0); // absent → zero, not NaN
    }

    #[test]
    fn extract_rejects_non_create_txtype() {
        let mut v = long_shape();
        v["txType"] = serde_json::json!("buy");
        assert!(extract_create(&v).is_none());
    }

    #[test]
    fn extract_rejects_mint_without_reserves() {
        // Stale-alias alarm path: mint present, reserves missing.
        let v = serde_json::json!({"mint": "MINT3333333333333333333333333333333333333"});
        assert!(extract_create(&v).is_none());
        let v = serde_json::json!({"mint": "M", "vSol": 0.0, "vTokens": 5.0});
        assert!(extract_create(&v).is_none());
    }

    #[test]
    fn num_flex_accepts_int_float_and_string() {
        assert_eq!(num_flex(&serde_json::json!(42u64)), Some(42.0));
        assert_eq!(num_flex(&serde_json::json!(1.5)), Some(1.5));
        assert_eq!(num_flex(&serde_json::json!("3.25")), Some(3.25));
        assert_eq!(num_flex(&serde_json::json!(true)), None);
    }

    #[test]
    fn enrichment_math_on_synthetic_reserves() {
        // 30.099 SOL vs 1,069,470,314.15694 tokens → ~2.8143e-8 SOL/token;
        // SOL=$200 → price ≈ $5.63e-6, liquidity = 2·30.099·200 = $12,039.6.
        // Creator: 3,529,685.84306 tokens ÷ 1e9 = 0.352968584306%.
        let c = extract_create(&long_shape()).unwrap();
        let en = enrich_create(&c, Utc::now(), dec!(200)).unwrap();
        let hold = en.creator_hold_pct;
        assert!(hold > dec!(0.3529) && hold < dec!(0.3530), "hold={hold}");
        let px = en.price_sol_per_token;
        assert!(px > dec!(0.000000028) && px < dec!(0.0000000285), "px={px}");
        match &en.event {
            Event::Launch(l) => {
                assert_eq!(l.creator_hold_pct, hold);
                assert!(l.liquidity_usd > dec!(12039) && l.liquidity_usd < dec!(12041));
                assert!(l.price_usd > dec!(0.0000056) && l.price_usd < dec!(0.0000057));
                assert!(l.on_curve);
                assert!(!l.mint_renounced); // unvetted placeholder
                assert!(!l.is_honeypot); // unvetted placeholder
            }
            _ => panic!("expected launch"),
        }
    }

    #[test]
    fn enrichment_zero_buy_is_exactly_zero() {
        let c = PumpPortalCreate {
            mint: "M".into(),
            signature: String::new(),
            trader: String::new(),
            bonding_curve: "C".into(),
            vtokens_tokens: 1_073_000_000.0,
            vsol_sol: 30.0,
            initial_buy_tokens: 0.0,
            sol_amount: 0.0,
            market_cap_sol: 30.0,
            name: String::new(),
            symbol: String::new(),
            uri: String::new(),
            pool: String::new(),
            mayhem: false,
        };
        let en = enrich_create(&c, Utc::now(), dec!(200)).unwrap();
        assert_eq!(en.creator_hold_pct, Decimal::ZERO);
        assert_eq!(en.price_sol_per_token, dec!(30) / dec!(1073000000));
    }

    #[test]
    fn enrichment_fails_closed() {
        let mut c = PumpPortalCreate {
            mint: "M".into(),
            signature: String::new(),
            trader: String::new(),
            bonding_curve: "C".into(),
            vtokens_tokens: 1_000_000.0,
            vsol_sol: 30.0,
            initial_buy_tokens: 0.0,
            sol_amount: 0.0,
            market_cap_sol: 30.0,
            name: String::new(),
            symbol: String::new(),
            uri: String::new(),
            pool: String::new(),
            mayhem: false,
        };
        assert!(enrich_create(&c, Utc::now(), Decimal::ZERO).is_none()); // bad rate
        assert!(enrich_create(&c, Utc::now(), dec!(-1)).is_none()); // negative rate
        c.initial_buy_tokens = 1_500_000_000.0; // > supply: impossible
        assert!(enrich_create(&c, Utc::now(), dec!(200)).is_none());
        c.initial_buy_tokens = f64::NAN; // non-finite wire value
        assert!(enrich_create(&c, Utc::now(), dec!(200)).is_none());
        c.initial_buy_tokens = 0.0;
        c.vsol_sol = 0.0; // drained reserves
        assert!(enrich_create(&c, Utc::now(), dec!(200)).is_none());
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(next_backoff_secs(1), 2);
        assert_eq!(next_backoff_secs(30), 60);
        assert_eq!(next_backoff_secs(60), 60);
        assert_eq!(next_backoff_secs(u64::MAX), 60); // no overflow
    }

    #[test]
    fn raw_of_create_has_no_slot_and_hexes_trader_when_possible() {
        let mut c = extract_create(&long_shape()).unwrap();
        let raw = raw_of_create(&c);
        assert_eq!(raw.slot, None);
        assert_eq!(raw.mint, c.mint);
        assert!(matches!(raw.kind, RawKind::Create { .. }));
        // Non-base58 trader passes through unchanged (never crashes).
        c.trader = "!!!not-base58!!!".into();
        let raw2 = raw_of_create(&c);
        match raw2.kind {
            RawKind::Create { creator_hex, .. } => assert_eq!(creator_hex, "!!!not-base58!!!"),
            _ => panic!("expected create"),
        }
    }

    /// Trade shape: create reserves with a buy side (pumpportal trade ticks
    /// carry the same reserve fields — no captured trade sample exists yet,
    /// so this is synthetic-but-plausible; the unparsed counter alarms if
    /// the real shape differs).
    fn buy_shape() -> serde_json::Value {
        let mut v = long_shape();
        v["txType"] = serde_json::json!("buy");
        v
    }

    #[test]
    fn parse_routes_trades_by_txtype() {
        assert!(matches!(
            parse_ws_line(&buy_shape().to_string()),
            WsInbound::TradeAttempt(_)
        ));
        let mut v = buy_shape();
        v["txType"] = serde_json::json!("sell");
        assert!(matches!(
            parse_ws_line(&v.to_string()),
            WsInbound::TradeAttempt(_)
        ));
        // Create + untyped mint lines stay on the create path.
        assert!(matches!(
            parse_ws_line(&long_shape().to_string()),
            WsInbound::CreateAttempt(_)
        ));
        assert!(matches!(
            parse_ws_line(&short_shape().to_string()),
            WsInbound::CreateAttempt(_)
        ));
        // Unknown side with a mint: create path rejects it into the unparsed
        // alarm — loud, never silently swallowed.
        let mut v = long_shape();
        v["txType"] = serde_json::json!("migrate");
        assert!(matches!(
            parse_ws_line(&v.to_string()),
            WsInbound::CreateAttempt(_)
        ));
        assert!(extract_create(&v).is_none());
    }

    #[test]
    fn extract_trade_side_and_reserves() {
        let t = extract_trade(&buy_shape()).unwrap();
        assert!(t.buy);
        assert_eq!(t.mint, "MINT1111111111111111111111111111111111111");
        let mut v = buy_shape();
        v["txType"] = serde_json::json!("sell");
        assert!(!extract_trade(&v).unwrap().buy);
        // Rejects: create side, missing side, drained reserves.
        assert!(extract_trade(&long_shape()).is_none());
        let mut v = buy_shape();
        v.as_object_mut().unwrap().remove("txType");
        assert!(extract_trade(&v).is_none());
        let mut v = buy_shape();
        v["vSolInBondingCurve"] = serde_json::json!(0.0);
        assert!(extract_trade(&v).is_none());
    }

    #[test]
    fn trade_price_agrees_with_create_price() {
        // One reserve formula for both paths: a tick and a create at the same
        // reserves must produce the same price.
        let at = Utc::now();
        let rate = dec!(200);
        let c = extract_create(&long_shape()).unwrap();
        let t = extract_trade(&buy_shape()).unwrap();
        let launch_px = match enrich_create(&c, at, rate).unwrap().event {
            Event::Launch(l) => l.price_usd,
            _ => panic!("expected launch"),
        };
        let tick = enrich_trade(&t, at, rate).unwrap();
        let tick_px = match &tick {
            Event::Price(p) => p.price_usd,
            _ => panic!("expected price"),
        };
        assert_eq!(launch_px, tick_px);
        match tick {
            Event::Price(p) => {
                assert_eq!(p.mint, t.mint);
                assert_eq!(p.ts, at);
                assert!(p.liquidity_usd > dec!(12039) && p.liquidity_usd < dec!(12041));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn enrich_trade_fails_closed() {
        let t = extract_trade(&buy_shape()).unwrap();
        assert!(enrich_trade(&t, Utc::now(), Decimal::ZERO).is_none());
        let mut bad = t.clone();
        bad.vsol_sol = 0.0;
        assert!(enrich_trade(&bad, Utc::now(), dec!(200)).is_none());
        bad.vsol_sol = f64::NAN; // bypasses extract's finite check
        assert!(enrich_trade(&bad, Utc::now(), dec!(200)).is_none());
    }

    #[test]
    fn raw_of_trade_uses_wstrade_variant() {
        let t = extract_trade(&buy_shape()).unwrap();
        let raw = raw_of_trade(&t);
        assert_eq!(raw.slot, None);
        assert_eq!(raw.mint, t.mint);
        match raw.kind {
            RawKind::WsTrade { bonding_curve, buy } => {
                assert!(buy);
                assert_eq!(bonding_curve, t.bonding_curve);
            }
            _ => panic!("expected WsTrade"),
        }
    }

    #[test]
    fn sub_messages_carry_method_and_keys() {
        let keys = vec!["A".to_string(), "B".to_string()];
        let v: serde_json::Value = serde_json::from_str(&subscribe_token_trade_msg(&keys)).unwrap();
        assert_eq!(v["method"], "subscribeTokenTrade");
        assert_eq!(v["keys"], serde_json::json!(["A", "B"]));
        let v: serde_json::Value =
            serde_json::from_str(&unsubscribe_token_trade_msg(&keys)).unwrap();
        assert_eq!(v["method"], "unsubscribeTokenTrade");
        assert_eq!(v["keys"], serde_json::json!(["A", "B"]));
    }

    #[test]
    fn plan_subs_covers_open_unsubs_closed() {
        let open = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let sub = HashSet::new();
        let (to_sub, to_unsub) = plan_trade_subs(&open, &sub, 25);
        assert_eq!(to_sub, open);
        assert!(to_unsub.is_empty());
        // Steady state: no churn.
        let sub: HashSet<String> = open.iter().cloned().collect();
        let (to_sub, to_unsub) = plan_trade_subs(&open, &sub, 25);
        assert!(to_sub.is_empty() && to_unsub.is_empty());
        // One closed → exact unsub, nothing else moves.
        let open2 = vec!["b".to_string(), "c".to_string()];
        let (to_sub, to_unsub) = plan_trade_subs(&open2, &sub, 25);
        assert!(to_sub.is_empty());
        assert_eq!(to_unsub, vec!["a".to_string()]);
    }

    #[test]
    fn plan_subs_cap_drops_new_never_evicts() {
        // Cap pressure: survivors keep slots, excess NEW mints wait.
        let open = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let sub: HashSet<String> = ["b".to_string()].into_iter().collect();
        let (to_sub, to_unsub) = plan_trade_subs(&open, &sub, 2);
        assert_eq!(to_sub, vec!["a".to_string()]);
        assert!(to_unsub.is_empty());
        // Unsubs stay exact even when over cap.
        let open = vec!["a".to_string()];
        let sub: HashSet<String> = ["a".to_string(), "b".to_string(), "c".to_string()]
            .into_iter()
            .collect();
        let (to_sub, to_unsub) = plan_trade_subs(&open, &sub, 1);
        assert!(to_sub.is_empty());
        assert_eq!(to_unsub, vec!["b".to_string(), "c".to_string()]);
    }
}
