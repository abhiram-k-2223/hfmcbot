//! Strategy / decision module (spec §4): entry gate, funnel sizing, exit rules.
//!
//! Implemented as pure functions over per-token state so the same code paths run
//! live, in paper mode, and in the replay/backtest harness.

use crate::config::Config;
use crate::types::{HoldMode, Launch, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// Reasons the entry gate rejects a launch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RejectReason {
    #[error("token too old: age {age_secs}s > max {max_secs}s")]
    TooOld { age_secs: i64, max_secs: u64 },
    #[error("liquidity ${liq} below minimum ${min}")]
    TooIlliquid { liq: Decimal, min: Decimal },
    #[error("creator holds {hold}% > max {max}%")]
    DevHoldTooHigh { hold: Decimal, max: Decimal },
    #[error("mint not renounced on migrated pool (liquidity ${liq} >= ${min})")]
    NotRenouncedOnMigrated { liq: Decimal, min: Decimal },
    #[error("honeypot detected")]
    Honeypot,
}

/// Entry gate — ALL checks must pass (spec §4).
pub fn entry_gate(launch: &Launch, now: DateTime<Utc>, cfg: &Config) -> Result<(), RejectReason> {
    let age_secs = (now - launch.created_at).num_seconds();
    if age_secs > cfg.entry_max_age_secs as i64 {
        return Err(RejectReason::TooOld {
            age_secs,
            max_secs: cfg.entry_max_age_secs,
        });
    }
    if launch.liquidity_usd < cfg.min_liquidity_usd && !launch.on_curve {
        // Liquidity litmus: tokens still on their bonding curve are fine; a
        // *graduated* pool must carry real liquidity (spec §4).
        return Err(RejectReason::TooIlliquid {
            liq: launch.liquidity_usd,
            min: cfg.min_liquidity_usd,
        });
    }
    if launch.creator_hold_pct > cfg.max_dev_hold_pct {
        return Err(RejectReason::DevHoldTooHigh {
            hold: launch.creator_hold_pct,
            max: cfg.max_dev_hold_pct,
        });
    }
    if launch.is_honeypot {
        return Err(RejectReason::Honeypot);
    }
    if !launch.on_curve && !launch.mint_renounced {
        // Spec: "skip if ... mint not renounced on migrated pool".
        return Err(RejectReason::NotRenouncedOnMigrated {
            liq: launch.liquidity_usd,
            min: cfg.min_liquidity_usd,
        });
    }
    Ok(())
}

/// Position sizing (risk-first, spec §4): per-token max =
/// min(risk_per_trade_pct * equity, max_single_pos_usd), further capped by
/// capital actually free.
pub fn position_budget(cfg: &Config, equity: Decimal, deployed_usd: Decimal) -> Decimal {
    let risk_cap = equity * cfg.risk_per_trade_pct / Decimal::from(100);
    let hard_cap = cfg.max_single_pos_usd;
    let mut budget = risk_cap.min(hard_cap);
    let free = equity - deployed_usd;
    if free < Decimal::ZERO || budget > free {
        budget = free.max(Decimal::ZERO);
    }
    budget
}

/// Split `total` into `n` funnel slices (spec §4: 2–3 slices back-to-back).
/// Every slice gets floor(total/n) at 4dp; the remainder rides on the LAST
/// slice so the slices sum exactly to `total`.
pub fn split_into_slices(total: Decimal, n: usize) -> Vec<Decimal> {
    assert!(n >= 1, "funnel needs at least one slice");
    if n == 1 || total <= Decimal::ZERO {
        return vec![total];
    }
    let base = (total / Decimal::from(n as u64)).floor().round_dp(4);
    if base <= Decimal::ZERO {
        // Not enough to split meaningfully — single slice.
        return vec![total];
    }
    let mut slices = vec![base; n - 1];
    let last = total - base * Decimal::from((n - 1) as u64);
    slices.push(last);
    slices
}

/// Exit decision produced by the exit engine.
#[derive(Debug, Clone, PartialEq)]
pub struct ExitDecision {
    pub reason: &'static str,
}

/// Update `pos` with a new price and return the exit decision, if any.
///
/// Flip mode (default for the spray, spec §4):
///   - take-profit at +`take_profit_pct` from entry → sell all;
///   - stop at `stop_loss_pct` below the high-water mark (continuous proxy for
///     "last higher close") → sell all;
///   - time stop after `max_hold_secs`.
///
/// Conviction mode (promoted on strong breakout):
///   - promoted when price is up >= `conviction_min_pct` from entry;
///   - trail a stop `trail_pct` below the high-water mark;
///   - hard cap `conviction_max_hold_secs`.
pub fn on_price(
    pos: &mut Position,
    price: Decimal,
    now: DateTime<Utc>,
    cfg: &Config,
) -> Option<ExitDecision> {
    if price > pos.high_water {
        pos.high_water = price;
    }
    let pnl_pct = pos.unrealized_pnl_pct(price);

    match pos.mode {
        HoldMode::Flip => {
            // Promotion check first: breakout winners move to trail mode.
            if pnl_pct >= cfg.conviction_min_pct {
                pos.mode = HoldMode::Conviction;
                tracing::info!(
                    mint = %pos.mint,
                    pnl_pct = %pnl_pct,
                    "promoting to conviction mode (trail)"
                );
                return None;
            }
            if pnl_pct >= cfg.take_profit_pct {
                return Some(ExitDecision { reason: "take_profit" });
            }
            let stop_price =
                pos.high_water * (Decimal::ONE - cfg.stop_loss_pct / Decimal::from(100));
            if price <= stop_price {
                return Some(ExitDecision { reason: "stop_loss" });
            }
            if (now - pos.opened_at).num_seconds() >= cfg.max_hold_secs as i64 {
                return Some(ExitDecision { reason: "max_hold" });
            }
            None
        }
        HoldMode::Conviction => {
            let trail_stop = pos.high_water * (Decimal::ONE - cfg.trail_pct / Decimal::from(100));
            if price <= trail_stop {
                return Some(ExitDecision { reason: "trail_stop" });
            }
            if (now - pos.opened_at).num_seconds() >= cfg.conviction_max_hold_secs as i64 {
                return Some(ExitDecision { reason: "conviction_max_hold" });
            }
            None
        }
    }
}

/// Time-only exits, evaluated independently of price ticks. The engine sweeps
/// these on every event so a quiet/stale feed can never strand a position
/// past its stop.
pub fn on_time(pos: &Position, now: DateTime<Utc>, cfg: &Config) -> Option<ExitDecision> {
    let elapsed = (now - pos.opened_at).num_seconds();
    match pos.mode {
        HoldMode::Flip if elapsed >= cfg.max_hold_secs as i64 => {
            Some(ExitDecision { reason: "max_hold" })
        }
        HoldMode::Conviction if elapsed >= cfg.conviction_max_hold_secs as i64 => {
            Some(ExitDecision { reason: "conviction_max_hold" })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Launchpad;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn launch() -> Launch {
        Launch {
            mint: "MINT".into(),
            launchpad: Launchpad::PumpFun,
            created_at: ts(0),
            creator_hold_pct: dec!(2),
            mint_renounced: true,
            is_honeypot: false,
            liquidity_usd: dec!(60000),
            on_curve: true,
            price_usd: dec!(0.001),
        }
    }

    fn pos(entry: Decimal, high: Decimal, mode: HoldMode, opened: i64) -> Position {
        Position {
            mint: "MINT".into(),
            launchpad: Launchpad::PumpFun,
            opened_at: ts(opened),
            entry_price: entry,
            qty: dec!(1_000_000),
            cost_usd: dec!(1000),
            high_water: high,
            last_price: high,
            last_liquidity_usd: dec!(1_000_000),
            mode,
            exit_attempts: 0,
            stuck: false,
        }
    }

    #[test]
    fn gate_passes_fresh_safe_launch() {
        let cfg = Config::paper_defaults();
        assert!(entry_gate(&launch(), ts(10), &cfg).is_ok());
    }

    #[test]
    fn gate_rejects_stale_launch() {
        let cfg = Config::paper_defaults();
        let err = entry_gate(&launch(), ts(601), &cfg).unwrap_err();
        assert!(matches!(err, RejectReason::TooOld { .. }));
    }

    #[test]
    fn gate_rejects_honeypot_and_dev() {
        let cfg = Config::paper_defaults();
        let mut l = launch();
        l.is_honeypot = true;
        assert_eq!(entry_gate(&l, ts(1), &cfg), Err(RejectReason::Honeypot));

        let mut l = launch();
        l.creator_hold_pct = dec!(25);
        assert!(matches!(
            entry_gate(&l, ts(1), &cfg),
            Err(RejectReason::DevHoldTooHigh { .. })
        ));
    }

    #[test]
    fn gate_requires_renounce_only_when_graduated() {
        let cfg = Config::paper_defaults();
        // On-curve token (low virtual liquidity) may keep mint authority.
        let mut l = launch();
        l.liquidity_usd = dec!(8000);
        l.mint_renounced = false;
        assert!(l.on_curve);
        assert!(entry_gate(&l, ts(1), &cfg).is_ok());

        // Graduated pool with unrenounced mint is a hard reject.
        let mut l = launch();
        l.on_curve = false;
        l.mint_renounced = false;
        assert!(matches!(
            entry_gate(&l, ts(1), &cfg),
            Err(RejectReason::NotRenouncedOnMigrated { .. })
        ));

        // Graduated pool without real liquidity fails the litmus.
        let mut l = launch();
        l.on_curve = false;
        l.liquidity_usd = dec!(8000);
        assert!(matches!(
            entry_gate(&l, ts(1), &cfg),
            Err(RejectReason::TooIlliquid { .. })
        ));
    }

    #[test]
    fn sizing_respects_risk_and_hard_caps() {
        let cfg = Config::paper_defaults();
        // 2.5% of 50k = 1250 < 5000 → risk cap binds.
        assert_eq!(position_budget(&cfg, dec!(50000), Decimal::ZERO), dec!(1250));

        // Tiny equity: free capital binds.
        assert_eq!(position_budget(&cfg, dec!(1000), Decimal::ZERO), dec!(25));

        // Over-deployed → nothing left.
        assert_eq!(position_budget(&cfg, dec!(50000), dec!(60000)), Decimal::ZERO);
    }

    #[test]
    fn funnel_slices_sum_exactly() {
        let slices = split_into_slices(dec!(100), 3);
        assert_eq!(slices.len(), 3);
        let sum: Decimal = slices.iter().copied().sum();
        assert_eq!(sum, dec!(100));
        // Remainder rides the last slice.
        assert_eq!(slices[0], slices[1]);
        assert!(slices[2] >= slices[0]);

        // Degenerate: total smaller than slices.
        let tiny = split_into_slices(dec!(0.02), 3);
        assert_eq!(tiny, vec![dec!(0.02)]);
    }

    #[test]
    fn exit_take_profit_and_stop() {
        let cfg = Config::paper_defaults();

        // +120% → TP.
        let mut p = pos(dec!(0.001), dec!(0.001), HoldMode::Flip, 0);
        let d = on_price(&mut p, dec!(0.0022), ts(60), &cfg).unwrap();
        assert_eq!(d.reason, "take_profit");

        // Dump 35% from the high water → stop.
        let mut p = pos(dec!(0.001), dec!(0.002), HoldMode::Flip, 0);
        let d = on_price(&mut p, dec!(0.0013), ts(120), &cfg).unwrap();
        assert_eq!(d.reason, "stop_loss");
    }

    #[test]
    fn exit_time_stop_in_flip_mode() {
        let cfg = Config::paper_defaults();
        let mut p = pos(dec!(1), dec!(1), HoldMode::Flip, 0);
        let d = on_price(&mut p, dec!(1.01), ts(21_601), &cfg).unwrap();
        assert_eq!(d.reason, "max_hold");
    }

    #[test]
    fn time_stop_fires_without_price_ticks() {
        let cfg = Config::paper_defaults();
        // No price input at all — pure clock-based exit.
        let p = pos(dec!(1), dec!(1), HoldMode::Flip, 0);
        assert!(on_time(&p, ts(21_599), &cfg).is_none());
        let d = on_time(&p, ts(21_600), &cfg).unwrap();
        assert_eq!(d.reason, "max_hold");

        let p = pos(dec!(1), dec!(4), HoldMode::Conviction, 0);
        assert!(on_time(&p, ts(15 * 86_400 - 1), &cfg).is_none());
        let d = on_time(&p, ts(15 * 86_400), &cfg).unwrap();
        assert_eq!(d.reason, "conviction_max_hold");
    }

    #[test]
    fn promotion_to_conviction_then_trail() {
        let cfg = Config::paper_defaults();
        let mut p = pos(dec!(1), dec!(1), HoldMode::Flip, 0);

        // +300% promotes; no exit yet even past TP threshold.
        assert!(on_price(&mut p, dec!(4), ts(60), &cfg).is_none());
        assert_eq!(p.mode, HoldMode::Conviction);

        // Trail stop: 25% below high water (4) = 3.0. 3.05 survives.
        assert!(on_price(&mut p, dec!(3.05), ts(70), &cfg).is_none());
        let d = on_price(&mut p, dec!(2.99), ts(80), &cfg).unwrap();
        assert_eq!(d.reason, "trail_stop");

        // Conviction holds ignore the flip time stop.
        let mut p = pos(dec!(1), dec!(4), HoldMode::Conviction, 0);
        assert!(on_price(&mut p, dec!(3.5), ts(25_000), &cfg).is_none());
    }
}

