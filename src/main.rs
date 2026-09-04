//! hfmcbot binary — paper-mode runner / replay backtester (M0–M3) + live
//! execution boot (M4).
//!
//! Usage:
//!   hfmcbot                       # paper mode with no feed attached (idle)
//!   hfmcbot data/events.jsonl     # replay a recorded launch feed (backtest, paper only)
//!   HFM_MODE=live hfmcbot         # arm the live executor (keypair required,
//!                                 # simulate-only unless HFM_SIMULATE_ONLY=false)
//!
//! Observability (M0): `/metrics` (Prometheus text) + `/healthz` on
//! `HFM_METRICS_ADDR`, periodic heartbeat logs, panic hook → tracing.

use hfmcbot::config::{Config, Mode};
use hfmcbot::engine::Engine;
use hfmcbot::exec::PaperExecutor;
use hfmcbot::ingest::{LaunchFeed, ReplayFeed};
use hfmcbot::metrics;
use hfmcbot::persist::AuditLog;
use hfmcbot::types::HoldMode;
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    metrics::install_panic_hook();

    let cfg = Config::from_env().map_err(|e| format!("config error: {e}"))?;

    // Operator keypair (M0): optional in paper, REQUIRED for live (M4).
    // Only the pubkey + source var are ever logged — never the secret.
    // Kept as a value (not just a log line) so live boot can arm the signer.
    let loaded_key = match hfmcbot::keys::load_keypair_opt() {
        Ok(k) => {
            match &k {
                Some(loaded) => tracing::info!(
                    pubkey = %loaded.pubkey_base58,
                    source = loaded.source.var_name(),
                    mode = %cfg.mode,
                    "operator key ready"
                ),
                None => tracing::warn!(
                    mode = %cfg.mode,
                    "no operator key set (HFM_SECRET_KEY/SECRET_KEY) — paper mode only"
                ),
            }
            k
        }
        Err(e) => {
            tracing::error!(error = %e, "operator key invalid");
            return Err(format!("operator key error: {e}").into());
        }
    };

    // Observability: snapshot shared with the /metrics server.
    let snapshot = Arc::new(Mutex::new(metrics::EngineSnapshot::default()));
    let metrics_addr = cfg.metrics_addr.clone();
    let heartbeat_secs = cfg.heartbeat_secs;
    {
        let snap = snapshot.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics::serve_metrics(metrics_addr, snap).await {
                tracing::error!(error = %e, "metrics server exited");
            }
        });
    }
    metrics::spawn_heartbeat(heartbeat_secs);

    // M6 alerting: log ALWAYS; POST to the webhook when configured. The
    // worker owns the receiver; `fire()` degrades to log-only without one.
    let alert_url = std::env::var("HFM_ALERT_WEBHOOK_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let alert_min_secs: u64 = std::env::var("HFM_ALERT_MIN_SECS")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            s.trim()
                .parse()
                .map_err(|_| format!("bad HFM_ALERT_MIN_SECS={s:?}: want seconds >= 0"))
        })
        .transpose()?
        .unwrap_or(300);
    let (alerter, alert_rx) = hfmcbot::alerts::Alerter::new(alert_url.is_some(), alert_min_secs);
    if let (Some(rx), Some(url)) = (alert_rx, alert_url) {
        tracing::info!("alert webhook armed");
        tokio::spawn(hfmcbot::alerts::run_worker(rx, url, "hfmcbot".into()));
    }
    if cfg.kill_switch {
        alerter.fire(
            hfmcbot::alerts::AlertKind::KillSwitch,
            "booted with HFM_KILL_SWITCH=true: entries halted, exits allowed",
        );
    }

    // Replay path: CLI arg wins over HFM_REPLAY_EVENTS_PATH.
    let replay_path = std::env::args()
        .nth(1)
        .or_else(|| cfg.replay_events_path.clone());

    // `mut` only when the pg mirror can attach; other builds pass `audit`
    // by value into the engine untouched.
    #[cfg_attr(not(feature = "pg"), allow(unused_mut))]
    let mut audit = AuditLog::open(std::path::Path::new(&cfg.audit_log_path))
        .map_err(|e| format!("cannot open audit log {}: {e}", cfg.audit_log_path))?;
    let audit_path = audit.path().to_path_buf();

    // Postgres audit mirror (`--features pg` + HFM_PG_DSN): the JSONL file
    // above stays the primary trail; Postgres is a queryable copy. Live mode
    // refuses to boot on a dead mirror (a diverging audit trail next to real
    // money is worse than downtime); paper warns and continues file-only.
    if !cfg.pg_dsn.trim().is_empty() {
        #[cfg(feature = "pg")]
        {
            match hfmcbot::pg_audit::PgMirror::connect(cfg.pg_dsn.trim()).await {
                Ok(mirror) => {
                    tracing::info!("pg audit mirror connected");
                    audit.set_mirror(mirror);
                }
                Err(e) => {
                    if cfg.mode == Mode::Live {
                        return Err(format!("live boot refused: {e}").into());
                    }
                    tracing::warn!(error = %e, "pg mirror dead — continuing file-only (paper)");
                }
            }
        }
        #[cfg(not(feature = "pg"))]
        {
            return Err("HFM_PG_DSN is set but this binary was built without --features pg (mirror compiled out)".into());
        }
    }

    // Shadow detection BEFORE engine construction: the state-snapshot path
    // defaults per loop kind, and resume must happen at construction (the
    // executor + audit log move into the engine).
    let soak_url = std::env::var("HFM_SOAK_WS_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let shadow = cfg.mode == Mode::Paper && replay_path.is_none() && soak_url.is_some();
    let live_loop = cfg.mode == Mode::Live || shadow;

    // M6 crash recovery: a snapshot from a previous run resumes the book
    // (positions, ledger, breaker state) instead of starting flat.
    let state_default: &str = if shadow {
        "data/shadow_state.json"
    } else {
        "data/live_state.json"
    };
    let state_path = std::env::var("HFM_STATE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| state_default.into());
    let resume: Option<hfmcbot::persist::EngineState> =
        match hfmcbot::persist::load_state(std::path::Path::new(&state_path)) {
            Ok(s) => Some(s),
            Err(e) if std::path::Path::new(&state_path).exists() => {
                if cfg.mode == Mode::Live {
                    // Starting FLAT while real positions may be open risks
                    // double-entering. Refuse; the operator resolves manually.
                    return Err(format!(
                        "live boot refused: unreadable state snapshot {state_path}: {e}"
                    )
                    .into());
                }
                tracing::warn!(
                    path = %state_path, error = %e,
                    "ignoring unreadable state snapshot (paper starts flat)"
                );
                None
            }
            Err(_) => None, // no snapshot yet — first boot.
        };
    if let Some(ref s) = resume {
        tracing::warn!(
            open = s.positions.len(),
            closed = s.closed.len(),
            equity = %s.equity_usd,
            path = %state_path,
            "resuming engine book from state snapshot"
        );
    }

    let engine = if cfg.mode == Mode::Live {
        // Replay is a paper-mode operation: driving a LIVE executor from a
        // canned file would fire real orders. Refuse loudly.
        if replay_path.is_some() {
            return Err(
                "HFM_MODE=live with a replay file: refusing — replay is paper-only (unset HFM_MODE or drop the file arg)".into(),
            );
        }
        let loaded = loaded_key.ok_or_else(|| {
            "HFM_MODE=live requires an operator keypair: set HFM_SECRET_KEY (or SECRET_KEY)"
                .to_string()
        })?;

        // Devnet/RPC self-check BEFORE arming real money: prove the configured
        // RPC answers with a fresh blockhash. Unreachable RPC = no boot.
        let rpc = hfmcbot::live::RpcClient::new(&cfg);
        match rpc.fetch_recent_blockhash().await {
            Ok((hash, slot)) => tracing::info!(
                blockhash = %hash.chars().take(12).collect::<String>(),
                slot,
                rpc = %cfg.rpc_url,
                "RPC self-check OK"
            ),
            Err(e) => {
                tracing::error!(error = %e, rpc = %cfg.rpc_url, "RPC self-check FAILED");
                return Err(format!("live boot refused: RPC unreachable: {e}").into());
            }
        }

        let live_ex = hfmcbot::live::LiveExecutor::armed_with_signer(&cfg, &loaded)
            .map_err(|e| format!("cannot arm live executor: {e}"))?;
        if cfg.simulate_only {
            tracing::warn!(
                "LIVE mode, SIMULATE-ONLY: swaps are assembled + signed but NOTHING is submitted (set HFM_SIMULATE_ONLY=false to send)"
            );
        } else {
            tracing::warn!(
                "LIVE mode, SENDING ENABLED: real orders via Jupiter/Jito — kill switch available (HFM_KILL_SWITCH=true)"
            );
        }
        tracing::info!("{}", live_ex.describe());
        match resume {
            Some(snap) => Engine::restore(cfg.clone(), Box::new(live_ex), audit, snap)
                .map_err(|e| format!("live boot refused: snapshot failed money invariants: {e}"))?,
            None => Engine::new(cfg.clone(), Box::new(live_ex), audit),
        }
    } else {
        match resume {
            Some(snap) => {
                match Engine::restore(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit, snap)
                {
                    Ok(eng) => eng,
                    Err(e) => {
                        // Paper has no funds at risk; the audit trail still
                        // holds every decision for replay-based recovery.
                        tracing::warn!(
                            error = %e,
                            "snapshot failed invariants — paper starts flat"
                        );
                        let audit2 = AuditLog::open(std::path::Path::new(&cfg.audit_log_path))
                            .map_err(|e| {
                                format!("cannot reopen audit log {}: {e}", cfg.audit_log_path)
                            })?;
                        Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit2)
                    }
                }
            }
            None => Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit),
        }
    };
    // Arm M6 alerting on the live/shadow/paper engine alike (restore() builds
    // a fresh engine, so arming happens here, once, for every path).
    let mut engine = engine.with_alerter(alerter.clone());

    let Some(path) = replay_path else {
        // No replay file: the WS loop feeds the engine live (M5). Live mode
        // always streams; paper mode streams only when HFM_SOAK_WS_URL opts
        // into the shadow loop (live data, PAPER executor, no funds at risk).
        // Otherwise paper idles exactly as before.
        // (`soak_url`/`shadow` were resolved before engine construction for
        // snapshot-resume; replay implies paper-without-shadow by construction
        // below — a live+replay combo was already refused at boot.)
        if live_loop {
            let url = soak_url.ok_or_else(|| {
                "HFM_MODE=live needs HFM_SOAK_WS_URL (the keyless stream that feeds the engine) — see .env.example".to_string()
            })?;
            if cfg.sol_usd <= Decimal::ZERO {
                return Err(
                    "live/shadow loop needs HFM_SOL_USD > 0 (operator rate for enrichment)".into(),
                );
            }
            let ws_cfg = hfmcbot::wsfeed::WsFeedConfig::from_parts(url.trim())
                .map_err(|e| format!("bad HFM_SOAK_WS_URL: {e}"))?;
            let stats = Arc::new(Mutex::new(hfmcbot::ingest::FeedStats::default()));
            let (raw_def, ev_def) = if shadow {
                ("data/shadow_raw.jsonl", "data/shadow_events.jsonl")
            } else {
                ("data/live_raw.jsonl", "data/live_events.jsonl")
            };
            let raw_path = std::env::var("HFM_RAW_PATH").unwrap_or_else(|_| raw_def.into());
            let events_path = std::env::var("HFM_EVENTS_PATH").unwrap_or_else(|_| ev_def.into());
            let state_every_secs: u64 = std::env::var("HFM_STATE_EVERY_SECS")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(|s| {
                    s.trim()
                        .parse()
                        .map_err(|_| format!("bad HFM_STATE_EVERY_SECS={s:?}: want seconds >= 0"))
                })
                .transpose()?
                .unwrap_or(60);
            if shadow {
                tracing::warn!(
                    "entering SHADOW loop: live feed drives the PAPER executor — full decision path, zero funds at risk"
                );
            } else {
                tracing::warn!(
                    "entering LIVE loop: live feed drives the armed executor ({})",
                    if cfg.simulate_only {
                        "simulate-only, nothing submitted"
                    } else {
                        "SENDING ENABLED"
                    }
                );
            }
            hfmcbot::liveloop::run_live_loop(
                &mut engine,
                hfmcbot::liveloop::LiveLoopArgs {
                    ws: ws_cfg,
                    sol_usd: cfg.sol_usd,
                    max_trade_subs: cfg.max_trade_subs,
                    raw_path: Some(raw_path.into()),
                    events_path: Some(events_path.into()),
                    state_path: Some(state_path.into()),
                    state_every_secs,
                    alerter: Some(alerter.clone()),
                    snapshot: snapshot.clone(),
                    stats,
                },
            )
            .await;
            println!("{}", engine.summary());
            println!("audit log: {}", audit_path.display());
            if let Ok(mut s) = snapshot.lock() {
                *s = engine.snapshot();
            }
            return Ok(());
        }
        tracing::warn!(
            mode = %cfg.mode,
            "no feed attached — pass a replay file to backtest (paper), set HFM_SOAK_WS_URL+HFM_SOL_USD for the shadow loop, or run the WS soak recorder: cargo run --bin record"
        );
        println!("{}", engine.summary());
        println!("audit log: {}", audit_path.display());
        if let Ok(mut s) = snapshot.lock() {
            *s = engine.snapshot();
        }
        return Ok(());
    };

    let mut feed = ReplayFeed::from_path(std::path::Path::new(&path))
        .map_err(|e| format!("replay feed error: {e}"))?;
    tracing::info!(events = feed.len(), file = %path, mode = %cfg.mode, "replaying events");

    let mut seen = 0usize;
    while let Some(ev) = feed.next_event() {
        engine.on_event(&ev).await;
        seen += 1;
    }
    if let Ok(mut s) = snapshot.lock() {
        *s = engine.snapshot();
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
