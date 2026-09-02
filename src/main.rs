//! hfmcbot binary — paper-mode runner / replay backtester (M0–M3).
//!
//! Usage:
//!   hfmcbot                       # paper mode with no feed attached (idle)
//!   hfmcbot data/events.jsonl     # replay a recorded launch feed (backtest)

use hfmcbot::config::Config;
use hfmcbot::engine::Engine;
use hfmcbot::exec::PaperExecutor;
use hfmcbot::ingest::{LaunchFeed, ReplayFeed};
use hfmcbot::persist::AuditLog;
use hfmcbot::types::HoldMode;
use rust_decimal::Decimal;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // .env first so tracing/config pick up local overrides.
    match dotenvy::dotenv() {
        Ok(path) => eprintln!("loaded env from {}", path.display()),
        Err(_) => eprintln!("no .env found; using defaults (see .env.example)"),
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let cfg = Config::from_env().map_err(|e| format!("config error: {e}"))?;

    // Replay path: CLI arg wins over HFM_REPLAY_EVENTS_PATH.
    let replay_path = std::env::args()
        .nth(1)
        .or_else(|| cfg.replay_events_path.clone());

    let audit = AuditLog::open(std::path::Path::new(&cfg.audit_log_path))
        .map_err(|e| format!("cannot open audit log {}: {e}", cfg.audit_log_path))?;
    let audit_path = audit.path().to_path_buf();

    let mut engine = Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit);

    let Some(path) = replay_path else {
        tracing::warn!(
            mode = %cfg.mode,
            "no feed attached — live Geyser/WS ingest lands in M1. Pass a replay file to backtest: hfmcbot data/events.jsonl"
        );
        println!("{}", engine.summary());
        println!("audit log: {}", audit_path.display());
        return Ok(());
    };

    let mut feed = ReplayFeed::from_path(std::path::Path::new(&path))
        .map_err(|e| format!("replay feed error: {e}"))?;
    tracing::info!(events = feed.len(), file = %path, mode = %cfg.mode, "replaying events");

    let mut seen = 0usize;
    while let Some(ev) = feed.next_event() {
        engine.on_event(&ev);
        seen += 1;
    }

    println!("processed {seen} events");
    println!("{}", engine.summary());
    println!("day realized P&L: ${}", engine.day_realized_pnl());
    if engine.kill_switch() {
        println!("!! CIRCUIT BREAKER TRIPPED (kill switch engaged) !!");
    }

    println!("\nclosed trades:");
    for t in engine.closed_trades() {
        let sign = if t.pnl_usd >= Decimal::ZERO { "+" } else { "-" };
        let mode = match t.mode {
            HoldMode::Flip => "flip",
            HoldMode::Conviction => "conviction",
        };
        println!(
            "  {:<12} {:<11} entry {:<12} exit {:<12} {sign}${:.2} [{}]",
            t.mint,
            mode,
            t.entry_price,
            t.exit_price,
            t.pnl_usd.abs(),
            t.exit_reason,
        );
    }

    println!("\naudit log: {}", audit_path.display());
    Ok(())
}
