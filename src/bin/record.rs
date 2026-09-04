//! Soak recorder: streams the keyless pumpportal WS (D13) and archives
//! everything to JSONL for the replay sign-off (Step 2).
//!
//! * `HFM_SOAK_WS_URL` (default [`PUMPPORTAL_WS_URL`]) — stream to subscribe
//!   to. (Named SOAK — `HFM_WS_URL` is the Solana pubsub endpoint.)
//! * `HFM_RAW_PATH` (default `data/ws_raw.jsonl`) — VERBATIM wire lines, the
//!   ground truth. Never filtered, never re-serialized: re-decoding later
//!   must see exactly what the socket delivered.
//! * `HFM_EVENTS_PATH` (default `data/ws_events.jsonl`) — decoded raw events
//!   (`record_raw` shape) plus engine-ready `Launch` events (`ReplayFeed`
//!   shape) when enrichment is on.
//! * `HFM_SOL_USD` (optional) — operator-supplied SOL/USD rate enabling
//!   enrichment. Absent = raw-only mode. The rate is logged at startup so a
//!   stale rate is visible in the run's own logs, and every enriched event
//!   carries the rate that priced it (`sol_usd_used` in the log line).
//!
//! Read-only by construction: this binary never touches keys, never signs,
//! never trades. Gap accounting (D13) is printed every minute from
//! [`FeedStats`]: messages, creates, malformed, chatter, unparsed, longest
//! silence, reconnects.

use hfmcbot::ingest::FeedStats;
use hfmcbot::wsfeed::{WsFeedConfig, PUMPPORTAL_WS_URL};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let url = std::env::var("HFM_SOAK_WS_URL").unwrap_or_else(|_| PUMPPORTAL_WS_URL.into());
    let cfg = WsFeedConfig::from_parts(&url).unwrap_or_else(|e| {
        eprintln!("record: {e}");
        std::process::exit(2);
    });
    let raw_path = std::env::var("HFM_RAW_PATH").unwrap_or_else(|_| "data/ws_raw.jsonl".into());
    let events_path =
        std::env::var("HFM_EVENTS_PATH").unwrap_or_else(|_| "data/ws_events.jsonl".into());
    let sol_usd = std::env::var("HFM_SOL_USD")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            Decimal::from_str(s.trim()).unwrap_or_else(|e| {
                eprintln!("record: bad HFM_SOL_USD '{s}': {e}");
                std::process::exit(2);
            })
        });
    if let Some(rate) = sol_usd {
        if rate <= Decimal::ZERO {
            eprintln!("record: HFM_SOL_USD must be positive");
            std::process::exit(2);
        }
    }

    let stats = Arc::new(Mutex::new(FeedStats::default()));
    let stats_view = Arc::clone(&stats);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            let s = stats_view.lock().unwrap().clone();
            tracing::info!(
                ws_messages = s.ws_messages,
                creates = s.creates,
                malformed = s.ws_malformed,
                chatter = s.ws_chatter,
                unparsed_create = s.ws_unparsed_create,
                max_gap_secs = s.max_gap_secs,
                reconnects = s.reconnects,
                "soak heartbeat"
            );
        }
    });

    tracing::info!(
        url = %cfg.url,
        raw_path = %raw_path,
        events_path = %events_path,
        sol_usd = ?sol_usd,
        "recorder starting (read-only)"
    );
    hfmcbot::wsfeed::run_ws_loop(cfg, raw_path.into(), events_path.into(), sol_usd, stats).await;
}
