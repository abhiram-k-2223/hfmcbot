//! End-to-end replay test (spec §7): a synthetic launch feed reproducing the
//! CLM6E4 pattern — a losing spray with one ANSEM-style outlier — fed through
//! the full pipeline (ingest → strategy → risk → paper execution → audit).
//!
//! Gate: the replay must capture the outlier winner, cut the losers fast, and
//! end with net non-negative P&L.

use chrono::{TimeZone, Utc};
use hfmcbot::config::Config;
use hfmcbot::engine::Engine;
use hfmcbot::exec::PaperExecutor;
use hfmcbot::ingest::{LaunchFeed, ReplayFeed};
use hfmcbot::persist::AuditLog;
use hfmcbot::types::{Event, Launch, Launchpad, PriceUpdate};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn ts(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
}

fn launch(mint: &str, at: i64, price: Decimal, launchpad: Launchpad) -> Event {
    Event::Launch(Launch {
        mint: mint.into(),
        launchpad,
        created_at: ts(at),
        creator_hold_pct: dec!(3),
        mint_renounced: false, // on-curve; fine while liquidity is low
        is_honeypot: false,
        liquidity_usd: dec!(8000),
        on_curve: true,
        price_usd: price,
    })
}

fn price(mint: &str, at: i64, p: Decimal) -> Event {
    Event::Price(PriceUpdate {
        mint: mint.into(),
        ts: ts(at),
        price_usd: p,
        liquidity_usd: dec!(8000),
    })
}

/// Build the CLM6E4-style spray: 6 losers + 1 100x outlier.
fn spray_events() -> Vec<Event> {
    let mut evs = vec![];
    // The losers: bleed after entry, cut by stop/time.
    for (i, mint) in ["STONK", "SPYX", "KIMCHI", "ANTHRP", "MANLET", "RUG1"]
        .iter()
        .enumerate()
    {
        let at = i as i64 * 2; // staggered by 2s (throttle window is 60s)
        evs.push(launch(mint, at, dec!(0.001), Launchpad::PumpFun));
        evs.push(price(mint, at + 600, dec!(0.0006))); // -40% → stop_loss
    }
    // The outlier: slow bleed, then 100x, then -30% trail stop.
    evs.push(launch("ANSEM", 12, dec!(0.000222), Launchpad::Stonkfun));
    evs.push(price("ANSEM", 600, dec!(0.0002))); // small dip, holds
    evs.push(price("ANSEM", 600_000, dec!(0.0118))); // +5300% → conviction
    evs.push(price("ANSEM", 1_200_000, dec!(0.449))); // ATH, high water
    evs.push(price("ANSEM", 1_300_000, dec!(0.30))); // -33% → trail_stop
    evs
}

#[tokio::test]
async fn spray_replay_captures_outlier_and_cuts_losers() {
    let mut cfg = Config::paper_defaults();
    // 7 launches × 3 slices land within the first minute; lift the throttle so
    // the whole spray gets out (default 6/min would deliberately funnel-limit).
    cfg.max_trades_per_min = 30;
    let dir = tempfile::tempdir().unwrap();
    let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
    let mut engine = Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit);

    let mut feed = ReplayFeed::from_events(spray_events());
    while let Some(ev) = feed.next_event() {
        engine.on_event(&ev).await;
    }

    let trades = engine.closed_trades();
    let losers = trades.iter().filter(|t| t.pnl_usd < Decimal::ZERO).count();
    let winners = trades.iter().filter(|t| t.pnl_usd > Decimal::ZERO).count();

    // Six losers cut fast (all by stop_loss), one outlier captured.
    assert_eq!(losers, 6, "all spray losers must be cut");
    assert_eq!(winners, 1, "the outlier must be captured");
    let ansem = trades.iter().find(|t| t.mint == "ANSEM").unwrap();
    assert_eq!(ansem.exit_reason, "trail_stop");
    assert_eq!(
        ansem.mode,
        hfmcbot::types::HoldMode::Conviction,
        "outlier must be held in conviction mode"
    );
    // ANSEM bought ~0.000222, trail-stopped near 0.30: massively positive.
    assert!(
        ansem.pnl_usd > dec!(100000),
        "outlier P&L should dominate, got {}",
        ansem.pnl_usd
    );

    // EV structure: few big wins >> many small losses → net positive equity.
    assert!(
        engine.equity() >= cfg.equity_usd,
        "net EV must be non-negative over the replay set: {}",
        engine.summary()
    );

    // Audit trail must contain the full story.
    let audit_text = std::fs::read_to_string(dir.path().join("audit.jsonl")).unwrap();
    assert!(audit_text.contains("\"kind\":\"decision\""));
    assert!(audit_text.contains("\"kind\":\"order\""));
    assert!(audit_text.contains("\"kind\":\"fill\""));
    assert!(audit_text.contains("\"kind\":\"trade_closed\""));
}

#[tokio::test]
async fn replay_feed_from_file_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    let mut jsonl = String::new();
    for ev in spray_events() {
        jsonl.push_str(&serde_json::to_string(&ev).unwrap());
        jsonl.push('\n');
    }
    std::fs::write(&path, jsonl).unwrap();

    let mut cfg = Config::paper_defaults();
    cfg.max_trades_per_min = 30;
    let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
    let mut engine = Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit);
    let mut feed = ReplayFeed::from_path(&path).unwrap();
    assert_eq!(feed.len(), 6 * 2 + 5);
    while let Some(ev) = feed.next_event() {
        engine.on_event(&ev).await;
    }
    assert_eq!(engine.closed_trades().len(), 7);
}
