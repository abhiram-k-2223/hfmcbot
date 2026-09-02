//! Data ingestion (spec §3). M0–M3 ships the `LaunchFeed` trait plus a
//! deterministic `ReplayFeed` (backtest harness over recorded events). The
//! live Geyser/WebSocket feed plugs into the same trait in M1.

use crate::types::Event;
use std::collections::VecDeque;
use std::io::BufRead;
use std::path::Path;

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

/// Live feed (M1): Geyser gRPC or provider WebSocket for pump.fun/stonkfun
/// `create` + `trade` events. Intentionally unimplemented in M0–M3; the engine
/// refuses to boot in live mode until this lands.
pub struct LiveFeed;

impl LaunchFeed for LiveFeed {
    fn next_event(&mut self) -> Option<Event> {
        unimplemented!("live Geyser/WS feed lands in M1 (see BUILD_PROMPT.md §3)");
    }
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
}
