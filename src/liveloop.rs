//! Live trading loop (M5): keyless-WS creates + per-token trade ticks drive the
//! engine with a real executor behind the trait.
//!
//! Data path: `subscribeNewToken` creates → [`enrich_create`] → engine;
//! per-token `subscribeTokenTrade` ticks → [`enrich_trade`] → engine. Trade
//! subscriptions track the open book ([`plan_trade_subs`], D13 fair-use):
//! only held tokens are watched, stale subs are dropped exactly, and cap
//! pressure drops NEW subs rather than evicting held ones (an unsubscribed
//! open position then exits on its time stop — safe degradation, not a
//! missed exit).
//!
//! Two modes share this loop, chosen by the caller (main.rs):
//! * **shadow** — paper executor on the live feed (live data, simulated
//!   money). The safe soak: full decision path, zero funds at risk.
//! * **live** — armed `LiveExecutor` (simulate-only or sending).
//!
//! Archive honesty (same dual format as the recorder): raw lines go verbatim
//! to the raw file and typed raws + enriched events to the events file. The
//! events file replays through the Step-2 `"type"`-tag filter, bit-identical
//! (see [`archive_typed`]). Counters live in the
//! shared [`FeedStats`]; the engine snapshot is pushed after every event so
//! `/metrics` reflects the live book.

use crate::engine::Engine;
use crate::ingest::{FeedStats, StampedRawEvent};
use crate::metrics::EngineSnapshot;
use crate::wsfeed::{
    enrich_create, enrich_trade, extract_create, extract_trade, parse_ws_line, plan_trade_subs,
    raw_of_create, raw_of_trade, subscribe_token_trade_msg, unsubscribe_token_trade_msg,
    WsFeedConfig, WsInbound,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Arguments for [`run_live_loop`]. `sol_usd` must be > 0 (enrichment needs a
/// rate — the operator's rate, logged per run); `max_trade_subs` bounds the
/// per-token trade subscriptions (D13). Archive paths are optional: `None`
/// runs fully in-memory (tests, ephemeral runs).
pub struct LiveLoopArgs {
    pub ws: WsFeedConfig,
    pub sol_usd: Decimal,
    pub max_trade_subs: usize,
    pub raw_path: Option<PathBuf>,
    pub events_path: Option<PathBuf>,
    /// Crash-safe book snapshots (M6). `None` disables snapshots entirely;
    /// `state_every_secs == 0` snapshots only on shutdown/disconnect.
    pub state_path: Option<PathBuf>,
    pub state_every_secs: u64,
    /// M6 alerting (snapshot failures page). `None` = log-only as today.
    pub alerter: Option<crate::alerts::Alerter>,
    pub snapshot: Arc<Mutex<EngineSnapshot>>,
    pub stats: Arc<Mutex<FeedStats>>,
}

impl LiveLoopArgs {
    fn save_enabled(&self) -> bool {
        self.state_every_secs > 0 && self.state_path.is_some()
    }
}

/// Checkpoint the engine book. Snapshot writes are atomic (tmp + rename), so
/// a crash here can only lose recency, never corrupt the last good state.
/// Failures are LOUD (warn) but never fatal to the loop — the audit trail
/// still records every decision for replay-based recovery.
fn save_book(engine: &Engine, args: &LiveLoopArgs) {
    let Some(ref path) = args.state_path else {
        return;
    };
    match engine.save_state(path) {
        Ok(()) => tracing::debug!(
            path = %path.display(),
            open = engine.open_positions(),
            "state snapshot saved"
        ),
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "state snapshot FAILED (loop continues on audit trail)"
            );
            // The crash-recovery story just broke — page (rate-limited) so
            // the operator fixes the disk before the crash that needs it.
            if let Some(ref a) = args.alerter {
                a.fire(
                    crate::alerts::AlertKind::SnapshotFailed,
                    format!("state snapshot {} failed: {e}", path.display()),
                );
            }
        }
    }
}

/// Build the 0–2 subscription control messages for one reconciliation step.
/// Pure (no I/O) so the send/no-send conditions are unit-testable: empty
/// lists produce no message — the loop never spams the server on steady
/// state. Unsubscribe goes first so a slot frees before we claim it.
pub fn sub_control_messages(to_sub: &[String], to_unsub: &[String]) -> Vec<String> {
    let mut msgs = Vec::with_capacity(2);
    if !to_unsub.is_empty() {
        msgs.push(unsubscribe_token_trade_msg(to_unsub));
    }
    if !to_sub.is_empty() {
        msgs.push(subscribe_token_trade_msg(to_sub));
    }
    msgs
}

/// Stream the WS until SIGINT/SIGTERM, driving `engine` with live creates +
/// trade ticks. Graceful shutdown finishes the in-flight message (same
/// guarantee as the recorder: raw archived, event applied, book consistent)
/// and prints the final scoreboard. The engine's audit log records every
/// decision, so a restart replays from the archives, not from memory.
pub async fn run_live_loop(engine: &mut Engine, args: LiveLoopArgs) {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    if args.sol_usd <= Decimal::ZERO {
        tracing::error!("live loop refused: sol_usd must be > 0 for enrichment");
        return;
    }
    tracing::info!(
        sol_usd = %args.sol_usd,
        max_trade_subs = args.max_trade_subs,
        url = %args.ws.url,
        "live loop starting (creates + per-token trade ticks)"
    );

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

    // Currently trade-subscribed mints. Re-sent in full after every
    // reconnect (the server forgets subscriptions on disconnect).
    let mut subscribed: HashSet<String> = HashSet::new();
    let mut backoff_secs = 1u64;
    let mut last_msg: Option<Instant> = None;
    loop {
        if *sd_rx.borrow_and_update() {
            break;
        }
        {
            let mut st = args.stats.lock().unwrap();
            st.reconnects += 1;
        }
        tracing::warn!(url = %args.ws.url, "live loop connecting");
        match tokio_tungstenite::connect_async(args.ws.url.as_str()).await {
            Ok((mut ws, _)) => {
                backoff_secs = 1;
                if let Err(e) = ws
                    .send(Message::Text(crate::wsfeed::SUBSCRIBE_NEW_TOKEN.into()))
                    .await
                {
                    tracing::warn!(error = %e, "create-subscribe send failed, reconnecting");
                } else {
                    // Re-claim trade subs the server forgot (empty on first
                    // connect — `sub_control_messages` sends nothing then).
                    let held: Vec<String> = {
                        let mut h: Vec<String> = subscribed.iter().cloned().collect();
                        h.sort();
                        h
                    };
                    if !held.is_empty() {
                        let msg = subscribe_token_trade_msg(&held);
                        if let Err(e) = ws.send(Message::Text(msg.into())).await {
                            tracing::warn!(error = %e, "trade-resubscribe failed, reconnecting");
                            // Fall through to the stream loop anyway: the
                            // next reconcile re-sends whatever is missing.
                        }
                    }
                    tracing::info!("live loop streaming");
                    if stream_loop(
                        &mut ws,
                        engine,
                        &args,
                        &mut subscribed,
                        &mut sd_rx,
                        &mut last_msg,
                    )
                    .await
                    {
                        // Shutdown signalled mid-stream: checkpoint the book,
                        // scoreboard + exit.
                        save_book(engine, &args);
                        final_stats(&args.stats, engine);
                        tracing::info!("shutdown during stream: exiting clean");
                        return;
                    }
                    // Disconnect (not shutdown): checkpoint before the gap —
                    // the book is most valuable right when the feed drops.
                    save_book(engine, &args);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, backoff_secs, "ws connect failed, retrying");
            }
        }
        tokio::select! {
            _ = sd_rx.changed() => break,
            _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
        }
        backoff_secs = crate::wsfeed::next_backoff_secs(backoff_secs);
    }
    save_book(engine, &args);
    final_stats(&args.stats, engine);
    tracing::info!("live loop stopped");
}

/// One connected stream session. Returns `true` when shutdown was signalled
/// (caller exits), `false` on disconnect/error (caller reconnects).
async fn stream_loop<S>(
    ws: &mut S,
    engine: &mut Engine,
    args: &LiveLoopArgs,
    subscribed: &mut HashSet<String>,
    sd_rx: &mut tokio::sync::watch::Receiver<bool>,
    last_msg: &mut Option<Instant>,
) -> bool
where
    S: futures_util::Sink<
            tokio_tungstenite::tungstenite::Message,
            Error = tokio_tungstenite::tungstenite::Error,
        > + futures_util::Stream<
            Item = Result<
                tokio_tungstenite::tungstenite::Message,
                tokio_tungstenite::tungstenite::Error,
            >,
        > + Unpin,
{
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;

    // Periodic book checkpoint (M6). The interval is created once per
    // connection so quiet streams still snapshot on schedule; `Skip` avoids
    // a save-burst after a processing stall. The leading tick fires
    // immediately and is consumed here, not saved.
    let mut saver = tokio::time::interval(Duration::from_secs(args.state_every_secs.max(1)));
    saver.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    saver.tick().await;

    loop {
        let msg = tokio::select! {
            _ = sd_rx.changed() => return true,
            m = ws.next() => m,
            _ = saver.tick(), if args.save_enabled() => {
                save_book(engine, args);
                continue;
            }
        };
        let Some(msg) = msg else { break }; // peer closed stream
        let now_wall = Utc::now().timestamp_millis();
        let now_mono = Instant::now();
        let gap_secs = last_msg
            .map(|t| now_mono.saturating_duration_since(t).as_secs())
            .unwrap_or(0);
        *last_msg = Some(now_mono);
        {
            let mut st = args.stats.lock().unwrap();
            if gap_secs > st.max_gap_secs {
                st.max_gap_secs = gap_secs;
            }
        }
        match msg {
            Ok(Message::Text(line)) => {
                let line = line.as_str();
                if let Some(ref raw_path) = args.raw_path {
                    append_line(raw_path, line);
                }
                {
                    let mut st = args.stats.lock().unwrap();
                    st.ws_messages += 1;
                }
                match parse_ws_line(line) {
                    WsInbound::Malformed => {
                        args.stats.lock().unwrap().ws_malformed += 1;
                    }
                    WsInbound::Chatter => {
                        args.stats.lock().unwrap().ws_chatter += 1;
                    }
                    WsInbound::TradeAttempt(v) => {
                        let at = DateTime::from_timestamp_millis(now_wall).unwrap_or_else(Utc::now);
                        match extract_trade(&v).and_then(|t| {
                            let raw = raw_of_trade(&t);
                            enrich_trade(&t, at, args.sol_usd).map(|ev| (raw, ev))
                        }) {
                            None => {
                                args.stats.lock().unwrap().ws_unparsed_trade += 1;
                            }
                            Some((raw, ev)) => {
                                args.stats.lock().unwrap().ws_trades += 1;
                                archive_typed(args, now_wall, raw, Some(&ev));
                                engine.on_event(&ev).await;
                                after_event(ws, engine, args, subscribed).await;
                            }
                        }
                    }
                    WsInbound::CreateAttempt(v) => match extract_create(&v) {
                        None => {
                            args.stats.lock().unwrap().ws_unparsed_create += 1;
                        }
                        Some(c) => {
                            args.stats.lock().unwrap().creates += 1;
                            tracing::info!(
                                mint = %c.mint,
                                symbol = %c.symbol,
                                initial_buy_tokens = %c.initial_buy_tokens,
                                "live create"
                            );
                            let at =
                                DateTime::from_timestamp_millis(now_wall).unwrap_or_else(Utc::now);
                            let raw = raw_of_create(&c);
                            match enrich_create(&c, at, args.sol_usd) {
                                None => tracing::warn!(
                                    mint = %c.mint,
                                    "enrichment skipped (assumption violated)"
                                ),
                                Some(en) => {
                                    archive_typed(args, now_wall, raw, Some(&en.event));
                                    engine.on_event(&en.event).await;
                                    after_event(ws, engine, args, subscribed).await;
                                }
                            }
                        }
                    },
                }
            }
            Ok(Message::Binary(_)) => {
                let mut st = args.stats.lock().unwrap();
                st.ws_messages += 1;
                st.ws_chatter += 1;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
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
    false
}

/// Post-event bookkeeping, after EVERY engine event (create or tick):
/// reconcile trade subs against the open book, then push the snapshot so
/// `/metrics` reflects the live book within one event.
async fn after_event<S>(
    ws: &mut S,
    engine: &Engine,
    args: &LiveLoopArgs,
    subscribed: &mut HashSet<String>,
) where
    S: futures_util::Sink<
            tokio_tungstenite::tungstenite::Message,
            Error = tokio_tungstenite::tungstenite::Error,
        > + Unpin,
{
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let open = engine.open_mints();
    let (to_sub, to_unsub) = plan_trade_subs(&open, subscribed, args.max_trade_subs);
    for msg in sub_control_messages(&to_sub, &to_unsub) {
        if let Err(e) = ws.send(Message::Text(msg.into())).await {
            tracing::warn!(error = %e, "sub control send failed (reconciled next event)");
            break; // keep `subscribed` unchanged: retry covers it
        }
    }
    // Sends succeeded (or there was nothing to send): commit the plan.
    if !to_sub.is_empty() || !to_unsub.is_empty() {
        for m in &to_unsub {
            subscribed.remove(m);
        }
        for m in &to_sub {
            subscribed.insert(m.clone());
        }
        tracing::info!(
            watching = subscribed.len(),
            subbed = ?to_sub,
            unsubbed = ?to_unsub,
            "trade subs reconciled"
        );
    }
    if let Ok(mut s) = args.snapshot.lock() {
        *s = engine.snapshot();
    }
}

/// Archive one typed raw plus its enriched event into the events file — the
/// same dual format as the recorder (verbatim wire stays in the raw file).
/// REPLAY NOTE: the events file mixes typed raws (no `"type"` tag) with
/// engine events (`"type": "launch"|"price"`). Replay selects the engine
/// lines (`grep '"type"'`) — the Step-2 workflow — which replays
/// bit-identically: every decision input is an enriched event, and typed
/// raws carry no additional engine information. No-op when no events path
/// is set (in-memory runs).
fn archive_typed(
    args: &LiveLoopArgs,
    at_ms: i64,
    raw: crate::ingest::RawFeedEvent,
    enriched: Option<&crate::types::Event>,
) {
    let Some(ref events_path) = args.events_path else {
        return;
    };
    let stamped = StampedRawEvent { at_ms, event: raw };
    if let Err(e) = crate::ingest::record_raw(events_path, &stamped) {
        tracing::warn!(error = %e, "record_raw failed");
    }
    if let Some(ev) = enriched {
        if let Err(e) = crate::ingest::record_event(events_path, ev) {
            tracing::warn!(error = %e, "record_event failed");
        }
    }
}

/// Append one verbatim line (plus newline). Same guarantee as the recorder:
/// never panics, warns on failure (a failed archive is data loss — loud).
fn append_line(path: &std::path::Path, line: &str) {
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
                tracing::warn!(error = %e, "live raw archive write failed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "live raw archive open failed"),
    }
}

/// Scoreboard on every exit path: counters + live book state.
fn final_stats(stats: &Arc<Mutex<FeedStats>>, engine: &Engine) {
    let s = stats.lock().unwrap().clone();
    tracing::info!(
        ws_messages = s.ws_messages,
        creates = s.creates,
        ws_trades = s.ws_trades,
        malformed = s.ws_malformed,
        chatter = s.ws_chatter,
        unparsed_create = s.ws_unparsed_create,
        unparsed_trade = s.ws_unparsed_trade,
        max_gap_secs = s.max_gap_secs,
        reconnects = s.reconnects,
        open_positions = engine.open_positions(),
        equity = %engine.equity(),
        "live loop stats"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_messages_empty_on_steady_state() {
        assert!(sub_control_messages(&[], &[]).is_empty());
    }

    #[test]
    fn sub_messages_unsub_before_sub() {
        let to_sub = vec!["n1".to_string()];
        let to_unsub = vec!["o1".to_string()];
        let msgs = sub_control_messages(&to_sub, &to_unsub);
        assert_eq!(msgs.len(), 2);
        let first: serde_json::Value = serde_json::from_str(&msgs[0]).unwrap();
        let second: serde_json::Value = serde_json::from_str(&msgs[1]).unwrap();
        // Slot frees before we claim it: unsub ships first.
        assert_eq!(first["method"], "unsubscribeTokenTrade");
        assert_eq!(first["keys"], serde_json::json!(["o1"]));
        assert_eq!(second["method"], "subscribeTokenTrade");
        assert_eq!(second["keys"], serde_json::json!(["n1"]));
    }

    #[test]
    fn sub_messages_single_side_only() {
        let only_sub = sub_control_messages(&["n".to_string()], &[]);
        assert_eq!(only_sub.len(), 1);
        let only_unsub = sub_control_messages(&[], &["o".to_string()]);
        assert_eq!(only_unsub.len(), 1);
    }

    // -- stream_loop harness: a scripted socket, no network -----------------

    use crate::config::Config;
    use crate::exec::PaperExecutor;
    use crate::ingest::{LaunchFeed, ReplayFeed};
    use crate::persist::AuditLog;
    use futures_util::{Sink, Stream};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};

    /// Scripted socket: replays queued inbound messages, captures outbound
    /// control sends. Implements exactly the Sink+Stream shape `stream_loop`
    /// requires, so the test drives the REAL loop body (parse → enrich →
    /// engine → reconcile → sub control), not a copy of it.
    struct MockWs {
        inbound: VecDeque<Result<Message, WsError>>,
        sent: Vec<String>,
    }

    impl Stream for MockWs {
        type Item = Result<Message, WsError>;
        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.get_mut().inbound.pop_front())
        }
    }

    impl Sink<Message> for MockWs {
        type Error = WsError;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
            let s = match &item {
                Message::Text(t) => t.to_string(),
                other => format!("{other:?}"),
            };
            self.get_mut().sent.push(s);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
    }

    const MINT: &str = "MINT99999999999999999999999999999999999999";

    fn create_line() -> String {
        // $60K liquidity (2·300·$100) clears the $50K paper litmus; 0.1%
        // creator hold clears the 10% gate.
        serde_json::json!({
            "mint": MINT,
            "txType": "create",
            "vTokensInBondingCurve": 1_073_000_000.0,
            "vSolInBondingCurve": 300.0,
            "initialBuy": 1_000_000.0,
            "solAmount": 0.0,
            "marketCapSol": 280.0,
            "name": "LOOP",
            "symbol": "LOOP",
            "pool": "pump"
        })
        .to_string()
    }

    fn trade_line() -> String {
        serde_json::json!({
            "mint": MINT,
            "txType": "buy",
            "vTokensInBondingCurve": 1_070_000_000.0,
            "vSolInBondingCurve": 301.0,
            "solAmount": 0.5,
            "marketCapSol": 281.0
        })
        .to_string()
    }

    fn test_args(
        dir: &tempfile::TempDir,
    ) -> (
        LiveLoopArgs,
        Arc<Mutex<EngineSnapshot>>,
        Arc<Mutex<FeedStats>>,
    ) {
        let snapshot = Arc::new(Mutex::new(EngineSnapshot::default()));
        let stats = Arc::new(Mutex::new(FeedStats::default()));
        let args = LiveLoopArgs {
            ws: WsFeedConfig::from_parts("ws://localhost:9").unwrap(),
            sol_usd: rust_decimal_macros::dec!(100),
            max_trade_subs: 25,
            raw_path: Some(dir.path().join("raw.jsonl")),
            events_path: Some(dir.path().join("events.jsonl")),
            // Disabled by default in tests (no timer interference); the
            // snapshot-path test below opts in explicitly.
            state_path: None,
            state_every_secs: 0,
            alerter: None,
            snapshot: snapshot.clone(),
            stats: stats.clone(),
        };
        (args, snapshot, stats)
    }

    fn test_engine(dir: &tempfile::TempDir) -> Engine {
        let cfg = Config::paper_defaults();
        let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
        Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit)
    }

    #[tokio::test]
    async fn loop_opens_position_subscribes_trades_and_archives() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = test_engine(&dir);
        let (args, snapshot, stats) = test_args(&dir);
        let events_path = dir.path().join("events.jsonl");

        let mut ws = MockWs {
            inbound: VecDeque::from([
                Ok(Message::Text(create_line().into())),
                Ok(Message::Text(trade_line().into())),
                Ok(Message::Close(None)),
            ]),
            sent: Vec::new(),
        };
        let (_sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
        let mut subscribed = HashSet::new();
        let mut last_msg = None;

        // Peer closes after two ticks → clean disconnect (false), not shutdown.
        let shutdown = stream_loop(
            &mut ws,
            &mut engine,
            &args,
            &mut subscribed,
            &mut sd_rx,
            &mut last_msg,
        )
        .await;
        assert!(!shutdown);

        // Engine: the create opened a paper position; the tick was processed.
        assert_eq!(engine.open_positions(), 1);
        let s = stats.lock().unwrap().clone();
        assert_eq!(s.creates, 1);
        assert_eq!(s.ws_trades, 1);
        assert_eq!(s.ws_unparsed_create, 0);
        assert_eq!(s.ws_unparsed_trade, 0);
        // Reconcile: the held mint got a trade sub via exactly one message.
        assert_eq!(subscribed, HashSet::from([MINT.to_string()]));
        assert_eq!(ws.sent.len(), 1);
        let sub: serde_json::Value = serde_json::from_str(&ws.sent[0]).unwrap();
        assert_eq!(sub["method"], "subscribeTokenTrade");
        assert_eq!(sub["keys"], serde_json::json!([MINT]));
        // Snapshot follows the live book.
        assert_eq!(snapshot.lock().unwrap().open_positions, 1);

        // Archives: verbatim raw ×2; events file = typed raw + launch +
        // typed raw + price (the Step-2 dual format).
        let raw = std::fs::read_to_string(dir.path().join("raw.jsonl")).unwrap();
        assert_eq!(raw.lines().count(), 2);
        let events = std::fs::read_to_string(&events_path).unwrap();
        assert_eq!(events.lines().count(), 4);

        // Bit-identical replay: the `"type"`-tag filter selects exactly the
        // two engine events; a fresh paper engine fed those reaches the same
        // book (1 open) and the same equity.
        let filtered: Vec<crate::types::Event> = events
            .lines()
            .filter(|l| l.contains("\"type\""))
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(filtered.len(), 2);
        let dir2 = tempfile::tempdir().unwrap();
        let mut replay = test_engine(&dir2);
        let mut feed = ReplayFeed::from_events(filtered);
        while let Some(ev) = feed.next_event() {
            replay.on_event(&ev).await;
        }
        assert_eq!(replay.open_positions(), engine.open_positions());
        assert_eq!(replay.equity(), engine.equity());
    }

    #[tokio::test]
    async fn loop_counts_unparsed_and_sends_nothing_without_events() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = test_engine(&dir); // empty book
        let (args, _snapshot, stats) = test_args(&dir);

        let mut ws = MockWs {
            inbound: VecDeque::from([
                // Stale-alias shape: mint + side, no reserves → alarm, kept raw.
                Ok(Message::Text(
                    serde_json::json!({"mint": MINT, "txType": "sell"})
                        .to_string()
                        .into(),
                )),
                Ok(Message::Close(None)),
            ]),
            sent: Vec::new(),
        };
        let (_sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
        let mut subscribed = HashSet::new();
        let mut last_msg = None;

        let shutdown = stream_loop(
            &mut ws,
            &mut engine,
            &args,
            &mut subscribed,
            &mut sd_rx,
            &mut last_msg,
        )
        .await;
        assert!(!shutdown);
        assert_eq!(stats.lock().unwrap().ws_unparsed_trade, 1);
        // No engine event ⇒ no reconcile ⇒ no control traffic at all.
        assert!(subscribed.is_empty());
        assert!(ws.sent.is_empty());
    }

    fn lethal_line() -> String {
        // Curve drained 30,000x: -99.997% tick → flip stop-loss exits.
        serde_json::json!({
            "mint": MINT,
            "txType": "sell",
            "vTokensInBondingCurve": 1_073_000_000.0,
            "vSolInBondingCurve": 0.01,
            "solAmount": 299.0,
            "marketCapSol": 0.001
        })
        .to_string()
    }

    #[tokio::test]
    async fn loop_closes_position_and_unsubs() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = test_engine(&dir);
        let (args, _snapshot, stats) = test_args(&dir);

        let mut ws = MockWs {
            inbound: VecDeque::from([
                Ok(Message::Text(create_line().into())),
                Ok(Message::Text(lethal_line().into())),
                Ok(Message::Close(None)),
            ]),
            sent: Vec::new(),
        };
        let (_sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
        let mut subscribed = HashSet::new();
        let mut last_msg = None;

        let shutdown = stream_loop(
            &mut ws,
            &mut engine,
            &args,
            &mut subscribed,
            &mut sd_rx,
            &mut last_msg,
        )
        .await;
        assert!(!shutdown);
        // Stop-loss closed the flip; the close-tick's reconcile dropped the sub.
        assert_eq!(engine.open_positions(), 0);
        assert_eq!(engine.closed_trades().len(), 1);
        assert_eq!(stats.lock().unwrap().ws_trades, 1); // the lethal tick
        assert!(subscribed.is_empty());
        assert_eq!(ws.sent.len(), 2);
        let sub: serde_json::Value = serde_json::from_str(&ws.sent[0]).unwrap();
        assert_eq!(sub["method"], "subscribeTokenTrade");
        let unsub: serde_json::Value = serde_json::from_str(&ws.sent[1]).unwrap();
        assert_eq!(unsub["method"], "unsubscribeTokenTrade");
        assert_eq!(unsub["keys"], serde_json::json!([MINT]));
    }

    #[tokio::test]
    async fn save_book_checkpoints_resumable_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = test_engine(&dir);
        let (mut args, _snapshot, _stats) = test_args(&dir);
        let state_path = dir.path().join("book").join("state.json");
        args.state_path = Some(state_path.clone());
        args.state_every_secs = 60;

        // Empty book checkpoints fine (restart with zero positions is valid).
        save_book(&engine, &args);
        let snap = crate::persist::load_state(&state_path).unwrap();
        assert_eq!(snap.positions.len(), 0);
        assert_eq!(snap.version, crate::persist::EngineState::VERSION);

        // After opening a position, the checkpoint carries the book, and a
        // fresh engine restored from it is invariant-clean.
        let mut ws = MockWs {
            inbound: VecDeque::from([
                Ok(Message::Text(create_line().into())),
                Ok(Message::Close(None)),
            ]),
            sent: Vec::new(),
        };
        let (_sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
        let mut subscribed = HashSet::new();
        let mut last_msg = None;
        stream_loop(
            &mut ws,
            &mut engine,
            &args,
            &mut subscribed,
            &mut sd_rx,
            &mut last_msg,
        )
        .await;
        assert_eq!(engine.open_positions(), 1);
        save_book(&engine, &args);

        let audit2 = AuditLog::open(&dir.path().join("audit2.jsonl")).unwrap();
        let cfg = Config::paper_defaults();
        let snap = crate::persist::load_state(&state_path).unwrap();
        let eng2 = Engine::restore(
            cfg.clone(),
            Box::new(PaperExecutor::new(&cfg)),
            audit2,
            snap,
        )
        .unwrap();
        assert_eq!(eng2.open_mints(), engine.open_mints());
        assert_eq!(eng2.equity(), engine.equity());
        eng2.check_invariants().unwrap();

        // Disabled (no path) saves nothing and fails nothing.
        args.state_path = None;
        save_book(&engine, &args);
    }
}
