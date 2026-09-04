//! Data ingestion (spec §3). M0–M3 ships the `LaunchFeed` trait plus a
//! deterministic `ReplayFeed` (backtest harness over recorded events). M1 adds
//! the read-only live path: Yellowstone gRPC → [`decode`] → [`LiveFeed`],
//! which buffers raw feed events for the recorder. The engine still refuses
//! `HFM_MODE=live` until the replay sign-off passes (Step 2) — nothing here
//! signs, trades, or decides.

use crate::decode;
use crate::types::Event;
use futures_util::SinkExt;
use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::Path;
use yellowstone_grpc_client::{GeyserGrpcClient, SubscribeRequestSink};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions,
};

/// Source of ingest events (launches + price updates), in chronological order.
pub trait LaunchFeed {
    /// Next event, or `None` when the feed is exhausted (replay) / not yet
    /// available (live implementations poll internally).
    fn next_event(&mut self) -> Option<Event>;
}

/// Replay/backtest feed: JSONL events (one JSON object per line, as produced by
/// `Event`'s serde format). Deterministic — same file, same decisions.
pub struct ReplayFeed {
    events: VecDeque<Event>,
}

impl ReplayFeed {
    /// Load events from a JSONL file, sorted by timestamp ascending.
    pub fn from_path(path: &Path) -> std::io::Result<ReplayFeed> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut events = Vec::new();
        for (lineno, line) in reader.lines().enumerate() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("//") {
                continue;
            }
            let ev: Event = serde_json::from_str(trimmed).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{}:{}: {}", path.display(), lineno + 1, e),
                )
            })?;
            events.push(ev);
        }
        events.sort_by_key(|e| e.ts());
        Ok(ReplayFeed {
            events: events.into(),
        })
    }

    pub fn from_events(events: Vec<Event>) -> ReplayFeed {
        let mut events = events;
        events.sort_by_key(|e| e.ts());
        ReplayFeed {
            events: events.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl LaunchFeed for ReplayFeed {
    fn next_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }
}

/// Live feed (M1, read-only): Yellowstone gRPC for pump.fun `create`/`trade`
/// events plus a stonkfun REST poller (see [`decode::STONKFUN_LAUNCHES_URL`]).
/// Decodes transactions into [`RawFeedEvent`]s buffered for the recorder;
/// enrichment into engine-ready [`Event`]s happens at replay-sign-off time
/// (Step 2), once holder/renounce/liquidity sources are calibrated.
///
/// The streaming loop ([`LiveFeed::run`]) is only reached by the soak runner —
/// `main.rs` never calls it and `HFM_MODE=live` is still refused at boot.
pub struct LiveFeed {
    cfg: LiveFeedConfig,
    buffer: VecDeque<StampedRawEvent>,
    clock: SlotClock,
    stats: FeedStats,
}

/// Connection settings. `token` is redacted in `Debug` (same hygiene as
/// `keys.rs`: secrets never hit logs).
pub struct LiveFeedConfig {
    pub endpoint: String,
    pub token: String,
    pub commitment: CommitmentLevel,
}

impl std::fmt::Debug for LiveFeedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveFeedConfig")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("commitment", &self.commitment)
            .finish()
    }
}

use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error("live feed not configured: set HFM_GEYSER_URL (and HFM_GEYSER_TOKEN)")]
    NotConfigured,
    #[error("bad HFM_FEED_COMMITMENT '{0}' (expected processed|confirmed|finalized)")]
    BadCommitment(String),
    #[error("geyser connect failed: {0}")]
    Connect(String),
    #[error("geyser stream broke: {0}")]
    Stream(String),
}

impl LiveFeedConfig {
    /// Build from the raw config strings; validates commitment, requires a
    /// non-empty endpoint. Token may be empty only for local/dev validators.
    pub fn from_parts(endpoint: &str, token: &str, commitment: &str) -> Result<Self, FeedError> {
        if endpoint.trim().is_empty() {
            return Err(FeedError::NotConfigured);
        }
        let commitment = match commitment {
            "processed" => CommitmentLevel::Processed,
            "confirmed" => CommitmentLevel::Confirmed,
            "finalized" => CommitmentLevel::Finalized,
            other => return Err(FeedError::BadCommitment(other.into())),
        };
        Ok(LiveFeedConfig {
            endpoint: endpoint.to_string(),
            token: token.to_string(),
            commitment,
        })
    }
}

/// A decoded instruction in proto-free form: the unit [`LiveFeed`] decodes.
/// `accounts` are base58 keys in instruction order, `data` the raw bytes.
#[derive(Debug, Clone)]
pub struct WireIx {
    pub program: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

/// A decoded feed event before enrichment (no price/liquidity yet — those
/// need curve reserves + SOL/USD + holder snapshots at sign-off time).
/// Serializable so the recorder can archive it verbatim for re-decoding.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawFeedEvent {
    /// Origin slot for gRPC-sourced events; `None` for the keyless WebSocket
    /// path (pumpportal sends no slot — wall-clock in [`StampedRawEvent`]
    /// is the only timestamp there).
    pub slot: Option<u64>,
    pub mint: String,
    pub kind: RawKind,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RawKind {
    Create {
        bonding_curve: String,
        creator_hex: String,
        name: String,
        symbol: String,
        mayhem: bool,
    },
    Trade {
        bonding_curve: String,
        buy: bool,
        v2: bool,
        amount_base_units: u64,
        limit_quote_units: u64,
    },
    /// Keyless-WS trade (M5 price-path feed): only what the WS types — side
    /// plus curve id. Reserve-derived price/liquidity live on the enriched
    /// `Event::Price`, not here; the verbatim raw line stays the ground truth
    /// in the archive. Separate variant (not zero-filled `Trade`) so a reader
    /// can never mistake "unknown" for "zero".
    WsTrade { bonding_curve: String, buy: bool },
    /// Graduation: curve migrated to a PumpSwap pool (ID `migrate` ix).
    Migrate { bonding_curve: String, pool: String },
}

/// Raw event stamped with wall-clock from [`SlotClock`] (block-time of its
/// slot). Events whose slot has no known block-time are dropped and counted —
/// a silent clock gap must never become a silently mis-timestamped trade.
#[derive(Debug, Clone)]
pub struct StampedRawEvent {
    pub at_ms: i64,
    pub event: RawFeedEvent,
}

/// Counters for feed health (exposed via `/metrics` at soak time).
#[derive(Debug, Default, Clone)]
pub struct FeedStats {
    pub creates: u64,
    pub trades: u64,
    pub migrates: u64,
    /// Instructions for the pump program with unrecognized discriminators —
    /// sustained >0 means the program was upgraded and decoders are stale.
    pub unknown_ix: u64,
    /// Events dropped for lack of a slot timestamp.
    pub unstamped_dropped: u64,
    /// Reconnects since start.
    pub reconnects: u64,
    /// WebSocket path (D13): raw text messages received.
    pub ws_messages: u64,
    /// WebSocket path: messages that weren't valid JSON.
    pub ws_malformed: u64,
    /// WebSocket path: valid JSON, no `mint` field (protocol chatter —
    /// subscribes-acks, heartbeats — not data loss).
    pub ws_chatter: u64,
    /// WebSocket path: had a `mint` but reserves/fields didn't parse (stale
    /// field aliases — alarm fire; the raw line is still archived verbatim).
    pub ws_unparsed_create: u64,
    /// WebSocket path: trade ticks decoded into `Event::Price` (M5).
    pub ws_trades: u64,
    /// WebSocket path: had a `mint` + buy/sell side but reserves didn't parse
    /// (same stale-alias alarm as creates, tracked separately so create
    /// health and price-path health are independently visible).
    pub ws_unparsed_trade: u64,
    /// WebSocket path: longest observed silence between messages (seconds).
    /// Gap accounting starts on day one per D13.
    pub max_gap_secs: u64,
}

/// Slot → block-time (unix millis) ring, fed by `blocks_meta` updates.
/// Bounded (`CLOCK_CAP`) so a long soak can't grow memory without bound.
#[derive(Debug, Default)]
pub struct SlotClock {
    inner: BTreeMap<u64, i64>,
}

const CLOCK_CAP: usize = 32_768;

impl SlotClock {
    pub fn insert(&mut self, slot: u64, at_ms: i64) {
        self.inner.insert(slot, at_ms);
        while self.inner.len() > CLOCK_CAP {
            let oldest = *self.inner.keys().next().unwrap();
            self.inner.remove(&oldest);
        }
    }

    /// Exact slot timestamp, falling back to the newest timestamp at or below
    /// the slot (slots usually arrive in order; the fallback covers harmless
    /// skew, never future stamps).
    pub fn lookup(&self, slot: u64) -> Option<i64> {
        self.inner.range(..=slot).next_back().map(|(_, ts)| *ts)
    }
}

/// Decode one transaction's pump-program instructions into raw events.
/// Returns `(events, unknown_ix_count)`; the caller stamps via [`SlotClock`].
pub fn handle_instructions(slot: u64, keys: &[String], ixs: &[WireIx]) -> (Vec<RawFeedEvent>, u64) {
    let mut events = Vec::new();
    let mut unknown = 0u64;
    for ix in ixs {
        if ix.program != decode::PUMP_PROGRAM {
            continue;
        }
        if let Some(c) = decode::parse_create(&ix.program, &ix.accounts, &ix.data) {
            events.push(RawFeedEvent {
                slot: Some(slot),
                mint: c.mint.clone(),
                kind: RawKind::Create {
                    bonding_curve: c.bonding_curve,
                    creator_hex: c.creator.iter().map(|b| format!("{b:02x}")).collect(),
                    name: c.name,
                    symbol: c.symbol,
                    mayhem: c.mayhem,
                },
            });
            continue;
        }
        if let Some(t) = decode::parse_trade(&ix.program, &ix.accounts, &ix.data) {
            events.push(RawFeedEvent {
                slot: Some(slot),
                mint: t.mint.clone(),
                kind: RawKind::Trade {
                    bonding_curve: t.bonding_curve,
                    buy: t.kind == decode::TradeKind::Buy,
                    v2: t.v2,
                    amount_base_units: t.amount_base_units,
                    limit_quote_units: t.limit_quote_units,
                },
            });
            continue;
        }
        if let Some(m) = decode::parse_migrate(&ix.program, &ix.accounts, &ix.data) {
            events.push(RawFeedEvent {
                slot: Some(slot),
                mint: m.mint.clone(),
                kind: RawKind::Migrate {
                    bonding_curve: m.bonding_curve,
                    pool: m.pool,
                },
            });
            continue;
        }
        unknown += 1;
    }
    let _ = keys;
    events.sort_by(|a, b| a.mint.cmp(&b.mint));
    (events, unknown)
}

impl LiveFeed {
    pub fn new(cfg: LiveFeedConfig) -> LiveFeed {
        LiveFeed {
            cfg,
            buffer: VecDeque::new(),
            clock: SlotClock::default(),
            stats: FeedStats::default(),
        }
    }

    pub fn stats(&self) -> &FeedStats {
        &self.stats
    }

    /// The exact subscription sent on connect: pump-program transactions
    /// (non-vote, non-failed) + `blocks_meta` for the slot clock. Pure
    /// constructor — unit-tested without any network.
    pub fn subscribe_request(&self) -> SubscribeRequest {
        let mut transactions = std::collections::HashMap::new();
        transactions.insert(
            "pumpfun".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![decode::PUMP_PROGRAM.to_string()],
                account_exclude: vec![],
                account_required: vec![],
                cuckoo_account_include: None,
                // No ATA/owner expansion: match the pump program only, so the
                // stream stays a launch/trade trickle instead of a firehose.
                token_accounts: None,
            },
        );
        let mut blocks_meta = std::collections::HashMap::new();
        blocks_meta.insert("clock".to_string(), SubscribeRequestFilterBlocksMeta {});
        SubscribeRequest {
            slots: Default::default(),
            accounts: Default::default(),
            transactions,
            transactions_status: Default::default(),
            blocks: Default::default(),
            blocks_meta,
            entry: Default::default(),
            commitment: Some(self.cfg.commitment as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }
    }

    /// Open the stream. Fails cleanly (no panic, no retry here) when the
    /// endpoint is unreachable — [`LiveFeed::run`] owns the backoff loop.
    pub async fn connect(
        &self,
    ) -> Result<
        (
            SubscribeRequestSink,
            impl tokio_stream::Stream<
                Item = Result<yellowstone_grpc_proto::geyser::SubscribeUpdate, tonic::Status>,
            >,
        ),
        FeedError,
    > {
        let token = if self.cfg.token.is_empty() {
            None
        } else {
            Some(self.cfg.token.clone())
        };
        let mut client = GeyserGrpcClient::build_from_shared(self.cfg.endpoint.clone())
            .map_err(|e| FeedError::Connect(e.to_string()))?
            .x_token(token)
            .map_err(|e| FeedError::Connect(e.to_string()))?
            .connect()
            .await
            .map_err(|e| FeedError::Connect(e.to_string()))?;
        let (mut tx, stream) = client
            .subscribe()
            .await
            .map_err(|e| FeedError::Connect(e.to_string()))?;
        tx.send(self.subscribe_request())
            .await
            .map_err(|e| FeedError::Connect(e.to_string()))?;
        Ok((tx, stream))
    }

    /// Soak-runner entry point: streams forever with backoff on disconnect,
    /// decoding into the buffer. Only called by the (future) recorder binary.
    pub async fn run(&mut self) -> ! {
        use tokio_stream::StreamExt;
        let mut backoff_secs = 1u64;
        loop {
            self.stats.reconnects += 1;
            tracing::warn!(endpoint = %self.cfg.endpoint, "live feed connecting");
            match self.connect().await {
                Ok((_tx, mut stream)) => {
                    backoff_secs = 1;
                    tracing::info!("live feed streaming");
                    while let Some(msg) = stream.next().await {
                        match msg {
                            Ok(update) => self.apply_update(update),
                            Err(e) => {
                                tracing::warn!(error = %e, "stream error, reconnecting");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, backoff_secs, "connect failed, retrying");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(60);
        }
    }

    fn apply_update(&mut self, update: yellowstone_grpc_proto::geyser::SubscribeUpdate) {
        match update.update_oneof {
            Some(UpdateOneof::Transaction(tx)) => self.apply_transaction(tx),
            Some(UpdateOneof::BlockMeta(meta)) => {
                if let Some(ts) = meta.block_time {
                    self.clock.insert(meta.slot, ts.timestamp * 1000);
                }
            }
            _ => {}
        }
    }

    fn apply_transaction(
        &mut self,
        tx: yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
    ) {
        let slot = tx.slot;
        let Some(info) = tx.transaction else {
            return;
        };
        let Some(msg) = info.transaction.and_then(|t| t.message) else {
            return;
        };
        let keys: Vec<String> = msg
            .account_keys
            .iter()
            .map(|k| bs58::encode(k).into_string())
            .collect();
        let get = |i: usize| keys.get(i).cloned().unwrap_or_default();
        let mut ixs = Vec::new();
        for ix in &msg.instructions {
            let program = get(ix.program_id_index as usize);
            if program.is_empty() {
                continue;
            }
            ixs.push(WireIx {
                program,
                accounts: ix.accounts.iter().map(|i| get(*i as usize)).collect(),
                data: ix.data.clone(),
            });
        }
        if let Some(meta) = info.meta {
            for group in &meta.inner_instructions {
                for ix in &group.instructions {
                    let program = get(ix.program_id_index as usize);
                    if program.is_empty() {
                        continue;
                    }
                    ixs.push(WireIx {
                        program,
                        accounts: ix.accounts.iter().map(|i| get(*i as usize)).collect(),
                        data: ix.data.clone(),
                    });
                }
            }
        }
        let (events, unknown) = handle_instructions(slot, &keys, &ixs);
        self.stats.unknown_ix += unknown;
        for ev in events {
            // gRPC-sourced events always carry a slot; the lookup's own
            // fallback covers skew, and a missing stamp drops loudly.
            let stamped = ev.slot.and_then(|s| self.clock.lookup(s));
            match stamped {
                Some(at_ms) => {
                    match &ev.kind {
                        RawKind::Create { .. } => self.stats.creates += 1,
                        RawKind::Trade { .. } => self.stats.trades += 1,
                        RawKind::Migrate { .. } => self.stats.migrates += 1,
                        // Unreachable by construction: `handle_instructions`
                        // only emits the three gRPC variants above (WsTrade
                        // is built solely on the keyless-WS path). Counted
                        // under the unknown tripwire anyway — if a decoder
                        // ever emits one here, the alarm fires instead of a
                        // silent miscount.
                        RawKind::WsTrade { .. } => self.stats.unknown_ix += 1,
                    }
                    self.buffer.push_back(StampedRawEvent { at_ms, event: ev });
                }
                None => self.stats.unstamped_dropped += 1,
            }
        }
    }

    /// Drain one buffered raw event (recorder consumes these).
    pub fn pop_raw(&mut self) -> Option<StampedRawEvent> {
        self.buffer.pop_front()
    }
}

impl LaunchFeed for LiveFeed {
    /// Live mode has no engine consumer yet (boot refusal stands): polling
    /// exists so the recorder and future paper-live share one interface.
    fn next_event(&mut self) -> Option<Event> {
        let _ = self.pop_raw();
        None
    }
}

/// Append one engine-ready event as JSONL (the exact [`ReplayFeed`] shape).
pub fn record_event(path: &Path, event: &Event) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut f, event)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    f.write_all(b"\n")?;
    Ok(())
}

/// Append one raw feed event as JSONL (verbatim archive for re-decoding and
/// late enrichment — the soak's most valuable artifact after the events).
pub fn record_raw(path: &Path, event: &StampedRawEvent) -> std::io::Result<()> {
    #[derive(serde::Serialize)]
    struct Archived<'a> {
        at_ms: i64,
        slot: Option<u64>,
        mint: &'a str,
        #[serde(flatten)]
        kind: &'a RawKind,
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let archived = Archived {
        at_ms: event.at_ms,
        slot: event.event.slot,
        mint: &event.event.mint,
        kind: &event.event.kind,
    };
    serde_json::to_writer(&mut f, &archived)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    f.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Launch, Launchpad, PriceUpdate};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(s: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.timestamp_opt(1_700_000_000 + s, 0).unwrap()
    }

    fn launch(mint: &str, at: i64) -> Event {
        Event::Launch(Launch {
            mint: mint.into(),
            launchpad: Launchpad::PumpFun,
            created_at: ts(at),
            creator_hold_pct: dec!(1),
            mint_renounced: true,
            is_honeypot: false,
            liquidity_usd: dec!(8000),
            on_curve: true,
            price_usd: dec!(0.001),
        })
    }

    #[test]
    fn replay_feed_sorts_by_timestamp() {
        let mut feed = ReplayFeed::from_events(vec![
            launch("B", 20),
            launch("A", 10),
            Event::Price(PriceUpdate {
                mint: "A".into(),
                ts: ts(5),
                price_usd: dec!(0.001),
                liquidity_usd: dec!(8000),
            }),
        ]);
        // Sorted: price@5, launch A@10, launch B@20.
        let e1 = feed.next_event().unwrap();
        assert!(matches!(e1, Event::Price(_)));
        let e2 = feed.next_event().unwrap();
        assert!(matches!(&e2, Event::Launch(l) if l.mint == "A"));
        let e3 = feed.next_event().unwrap();
        assert!(matches!(&e3, Event::Launch(l) if l.mint == "B"));
        assert!(feed.next_event().is_none());
    }

    #[test]
    fn replay_feed_reads_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(
            &path,
            concat!(
                r#"{"type":"launch","mint":"A","launchpad":"pumpfun","created_at":"2033-11-14T22:14:00Z","creator_hold_pct":"1","mint_renounced":true,"on_curve":true,"is_honeypot":false,"liquidity_usd":"8000","price_usd":"0.001"}"#,
                "\n",
                r#"{"type":"price","mint":"A","ts":"2033-11-14T22:15:00Z","price_usd":"0.002","liquidity_usd":"9000"}"#,
                "\n",
            ),
        )
        .unwrap();
        let mut feed = ReplayFeed::from_path(&path).unwrap();
        assert_eq!(feed.len(), 2);
        assert!(feed.next_event().is_some());
        assert!(feed.next_event().is_some());
        assert!(feed.next_event().is_none());
    }

    // ---- M1 live-feed tests (all offline: fixtures + pure constructors) ----

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn hex_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn wire_of(fixture_name: &str) -> (Vec<String>, WireIx) {
        let f = fixture(fixture_name);
        let accounts: Vec<String> = serde_json::from_value(f["accounts"].clone()).unwrap();
        let data = hex_bytes(f["data_hex"].as_str().unwrap());
        let ix = WireIx {
            program: decode::PUMP_PROGRAM.to_string(),
            accounts,
            data,
        };
        let keys = vec![decode::PUMP_PROGRAM.to_string()];
        (keys, ix)
    }

    fn feed_cfg() -> LiveFeedConfig {
        LiveFeedConfig::from_parts("https://example.invalid:10000", "sekrit-123", "processed")
            .unwrap()
    }

    #[test]
    fn feed_config_rejects_empty_endpoint_and_bad_commitment() {
        assert!(matches!(
            LiveFeedConfig::from_parts("", "tok", "processed"),
            Err(FeedError::NotConfigured)
        ));
        assert!(matches!(
            LiveFeedConfig::from_parts("https://x", "tok", "sometimes"),
            Err(FeedError::BadCommitment(_))
        ));
        // Token redacted in Debug, endpoint visible.
        let dbg = format!("{:?}", feed_cfg());
        assert!(!dbg.contains("sekrit-123"));
        assert!(dbg.contains("<redacted>"));
        assert!(dbg.contains("example.invalid"));
    }

    #[test]
    fn subscribe_request_filters_pump_program_only() {
        let feed = LiveFeed::new(feed_cfg());
        let req = feed.subscribe_request();
        let filt = req.transactions.get("pumpfun").unwrap();
        assert_eq!(filt.account_include, vec![decode::PUMP_PROGRAM.to_string()]);
        assert_eq!(filt.vote, Some(false));
        assert_eq!(filt.failed, Some(false));
        assert!(req.blocks_meta.contains_key("clock"));
        assert_eq!(req.commitment, Some(CommitmentLevel::Processed as i32));
    }

    #[test]
    fn slot_clock_exact_fallback_and_bound() {
        let mut clock = SlotClock::default();
        assert_eq!(clock.lookup(100), None);
        clock.insert(100, 1_000);
        clock.insert(200, 2_000);
        assert_eq!(clock.lookup(100), Some(1_000));
        assert_eq!(clock.lookup(150), Some(1_000)); // skew fallback, never future
        assert_eq!(clock.lookup(9_999_999), Some(2_000));
        for s in 0..(CLOCK_CAP as u64 + 10) {
            clock.insert(1_000_000 + s, s as i64);
        }
        assert!(clock.inner.len() <= CLOCK_CAP);
    }

    #[test]
    fn handle_instructions_decodes_live_buy() {
        let (keys, ix) = wire_of("pump_buy.json");
        let (events, unknown) = handle_instructions(4242, &keys, &[ix]);
        assert_eq!(unknown, 0);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].kind,
            RawKind::Trade {
                buy: true,
                v2: false,
                ..
            }
        ));
    }

    #[test]
    fn handle_instructions_counts_unknown_and_skips_foreign_programs() {
        let (keys, mut ix) = wire_of("pump_sell.json");
        ix.data[0..8].copy_from_slice(&[9u8; 8]); // vandalized discriminator
        let (events, unknown) = handle_instructions(1, &keys, &[ix.clone()]);
        assert!(events.is_empty());
        assert_eq!(unknown, 1);
        ix.program = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into();
        let (events, unknown) = handle_instructions(1, &keys, &[ix]);
        assert!(events.is_empty());
        assert_eq!(unknown, 0); // foreign programs aren't "unknown", they're ignored
    }

    #[test]
    fn recorder_round_trips_through_replay_feed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recorded.jsonl");
        // Out of order on purpose: recorder appends, ReplayFeed sorts.
        record_event(
            &path,
            &Event::Price(PriceUpdate {
                mint: "A".into(),
                ts: ts(30),
                price_usd: dec!(0.002),
                liquidity_usd: dec!(8000),
            }),
        )
        .unwrap();
        record_event(&path, &launch("A", 10)).unwrap();
        let mut feed = ReplayFeed::from_path(&path).unwrap();
        assert_eq!(feed.len(), 2);
        assert!(matches!(feed.next_event().unwrap(), Event::Launch(_)));
        assert!(matches!(feed.next_event().unwrap(), Event::Price(_)));
    }

    #[test]
    fn raw_archive_round_trips_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw.jsonl");
        let stamped = StampedRawEvent {
            at_ms: 1_729_000_000_000,
            event: RawFeedEvent {
                slot: Some(4242),
                mint: "MINT".into(),
                kind: RawKind::Trade {
                    bonding_curve: "CURVE".into(),
                    buy: true,
                    v2: false,
                    amount_base_units: 1_000_000,
                    limit_quote_units: 50_000,
                },
            },
        };
        record_raw(&path, &stamped).unwrap();
        let line = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["mint"], "MINT");
        assert_eq!(v["slot"], 4242);
        assert_eq!(v["kind"], "trade");
        assert_eq!(v["amount_base_units"], 1_000_000);
    }

    // ---- Synthetic SubscribeUpdate tests (M1 gRPC mapping, still offline) ----
    //
    // `apply_update` is only reachable over the network in production, so these
    // tests build the exact proto messages a validator would send (top-level +
    // inner instructions, block-time clock) from the live fixtures and drive
    // the private mapping directly. If Yellowstone ever reshapes the protos,
    // these fail at compile time — which is the point.

    use yellowstone_grpc_proto::geyser::{
        SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdateTransaction,
        SubscribeUpdateTransactionInfo,
    };
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        CompiledInstruction, InnerInstruction, InnerInstructions, Message, Transaction,
        TransactionStatusMeta, UnixTimestamp,
    };

    /// Account keys for a synthetic tx: index 0 is always the pump program,
    /// the rest are the fixture's accounts in order.
    fn synthetic_keys(accounts: &[String]) -> Vec<Vec<u8>> {
        let mut keys = vec![bs58::decode(decode::PUMP_PROGRAM).into_vec().unwrap()];
        for a in accounts {
            keys.push(bs58::decode(a).into_vec().unwrap());
        }
        keys
    }

    fn empty_meta() -> TransactionStatusMeta {
        TransactionStatusMeta {
            err: None,
            fee: 5000,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: vec![],
            inner_instructions_none: false,
            log_messages: vec![],
            log_messages_none: false,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses: vec![],
            loaded_readonly_addresses: vec![],
            return_data: None,
            return_data_none: false,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    /// One fixture ix as a top-level instruction in a synthetic tx update.
    fn synthetic_tx_update(accounts: &[String], data: Vec<u8>, slot: u64) -> SubscribeUpdate {
        let idx: Vec<u8> = (1..=accounts.len() as u8).collect();
        SubscribeUpdate {
            filters: vec!["pumpfun".into()],
            created_at: None,
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: vec![7u8; 64],
                    is_vote: false,
                    transaction: Some(Transaction {
                        signatures: vec![vec![7u8; 64]],
                        message: Some(Message {
                            header: None,
                            account_keys: synthetic_keys(accounts),
                            recent_blockhash: vec![0u8; 32],
                            instructions: vec![CompiledInstruction {
                                program_id_index: 0,
                                accounts: idx,
                                data,
                            }],
                            versioned: false,
                            address_table_lookups: vec![],
                            config: None,
                        }),
                    }),
                    meta: None,
                    index: 0,
                }),
            })),
        }
    }

    /// Mutate a synthetic update in place: drop top-level ixs, attach the
    /// fixture ix as a CPI inner-instruction group instead.
    fn move_ix_to_inner(update: &mut SubscribeUpdate, accounts: &[String], data: Vec<u8>) {
        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_mut() {
            if let Some(info) = tx.transaction.as_mut() {
                if let Some(txn) = info.transaction.as_mut() {
                    if let Some(msg) = txn.message.as_mut() {
                        msg.instructions.clear();
                    }
                }
                let mut meta = empty_meta();
                meta.inner_instructions = vec![InnerInstructions {
                    index: 0,
                    instructions: vec![InnerInstruction {
                        program_id_index: 0,
                        accounts: (1..=accounts.len() as u8).collect(),
                        data,
                        stack_height: Some(2),
                    }],
                }];
                info.meta = Some(meta);
            }
        }
    }

    fn block_meta_update(slot: u64, ts_secs: i64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["clock".into()],
            created_at: None,
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: String::new(),
                rewards: None,
                block_time: Some(UnixTimestamp { timestamp: ts_secs }),
                block_height: None,
                parent_slot: slot.saturating_sub(1),
                parent_blockhash: String::new(),
                executed_transaction_count: 1,
                entries_count: 1,
            })),
        }
    }

    fn fixture_accounts_and_data(name: &str) -> (Vec<String>, Vec<u8>) {
        let f = fixture(name);
        let accounts: Vec<String> = serde_json::from_value(f["accounts"].clone()).unwrap();
        let data = hex_bytes(f["data_hex"].as_str().unwrap());
        (accounts, data)
    }

    #[test]
    fn synthetic_tx_update_decodes_buy_and_stamps_from_clock() {
        let mut feed = LiveFeed::new(feed_cfg());
        feed.apply_update(block_meta_update(4242, 1_729_000_000));
        let (accounts, data) = fixture_accounts_and_data("pump_buy.json");
        feed.apply_update(synthetic_tx_update(&accounts, data, 4242));
        let stamped = feed.pop_raw().expect("buy should buffer");
        assert_eq!(stamped.at_ms, 1_729_000_000_000);
        assert_eq!(stamped.event.slot, Some(4242));
        assert!(matches!(
            stamped.event.kind,
            RawKind::Trade {
                buy: true,
                v2: false,
                ..
            }
        ));
        assert_eq!(feed.stats.trades, 1);
        assert_eq!(feed.stats.unstamped_dropped, 0);
        assert!(feed.pop_raw().is_none());
    }

    #[test]
    fn synthetic_tx_without_clock_entry_is_dropped_loudly() {
        let mut feed = LiveFeed::new(feed_cfg());
        let (accounts, data) = fixture_accounts_and_data("pump_sell.json");
        // No BlockMeta seen yet: the event must drop, never mis-stamp.
        feed.apply_update(synthetic_tx_update(&accounts, data.clone(), 9999));
        assert!(feed.pop_raw().is_none());
        assert_eq!(feed.stats.unstamped_dropped, 1);
        assert_eq!(feed.stats.trades, 0);
        // Late clock arrival stamps only *new* updates (no retro-fill by design).
        feed.apply_update(block_meta_update(9999, 1_729_000_100));
        feed.apply_update(synthetic_tx_update(&accounts, data, 9999));
        let stamped = feed.pop_raw().expect("sell should buffer after clock");
        assert_eq!(stamped.at_ms, 1_729_000_100_000);
        assert!(matches!(
            stamped.event.kind,
            RawKind::Trade { buy: false, .. }
        ));
    }

    #[test]
    fn synthetic_inner_instructions_are_mapped() {
        // CPI-emitted pump ixs (the migrate path on mainnet) must decode too.
        let mut feed = LiveFeed::new(feed_cfg());
        feed.apply_update(block_meta_update(5555, 1_729_000_200));
        let (accounts, data) = fixture_accounts_and_data("pump_sell.json");
        let mut update = synthetic_tx_update(&accounts, data.clone(), 5555);
        move_ix_to_inner(&mut update, &accounts, data);
        feed.apply_update(update);
        let stamped = feed.pop_raw().expect("inner sell should buffer");
        assert!(matches!(
            stamped.event.kind,
            RawKind::Trade { buy: false, .. }
        ));
        assert_eq!(feed.stats.trades, 1);
    }

    #[test]
    fn synthetic_out_of_range_program_index_skips_safely() {
        let mut feed = LiveFeed::new(feed_cfg());
        feed.apply_update(block_meta_update(1111, 1_729_000_300));
        let (accounts, data) = fixture_accounts_and_data("pump_buy.json");
        let mut update = synthetic_tx_update(&accounts, data, 1111);
        // Vandalize: program index points past the key list.
        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_mut() {
            if let Some(info) = tx.transaction.as_mut() {
                if let Some(txn) = info.transaction.as_mut() {
                    if let Some(msg) = txn.message.as_mut() {
                        msg.instructions[0].program_id_index = 250;
                    }
                }
            }
        }
        feed.apply_update(update);
        assert!(feed.pop_raw().is_none());
        assert_eq!(feed.stats.trades, 0);
        assert_eq!(feed.stats.unknown_ix, 0);
    }

    #[test]
    fn synthetic_non_transaction_updates_are_ignored() {
        let mut feed = LiveFeed::new(feed_cfg());
        feed.apply_update(SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        });
        // A tx update with no transaction payload is a no-op, not a crash.
        feed.apply_update(SubscribeUpdate {
            filters: vec!["pumpfun".into()],
            created_at: None,
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 1,
                transaction: None,
            })),
        });
        assert!(feed.pop_raw().is_none());
        assert_eq!(feed.stats.trades, 0);
        assert_eq!(feed.stats.creates, 0);
        assert_eq!(feed.stats.unstamped_dropped, 0);
    }
}
