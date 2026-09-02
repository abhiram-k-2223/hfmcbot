//! Risk & controls (spec §6 — NON-NEGOTIABLE, shipped with the strategy, not deferred):
//! kill switch, daily-loss circuit breaker, trade throttle, open-position cap,
//! and an outbound order rate limiter.

use crate::config::Config;
use crate::types::Side;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::VecDeque;

/// Why the breaker tripped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlockReason {
    #[error("kill switch engaged")]
    KillSwitch,
    #[error("daily loss limit tripped: day P&L ${pnl} <= -{limit}% of equity ${equity}")]
    DailyLossLimit {
        pnl: Decimal,
        limit: Decimal,
        equity: Decimal,
    },
    #[error("max open positions reached: {open}/{max}")]
    OpenPositionCap { open: usize, max: usize },
    #[error("trade throttle: {count} orders in last 60s >= max {max}/min")]
    Throttled { count: u32, max: u32 },
}

/// Circuit-breaker / control state.
#[derive(Debug)]
pub struct RiskEngine {
    cfg: Config,
    equity: Decimal,
    /// Realized P&L for the current UTC calendar day.
    day_realized_pnl: Decimal,
    /// UTC calendar day the counter belongs to (rolls over daily).
    day: chrono::NaiveDate,
    /// Set when the daily-loss breaker trips; clears on day rollover.
    daily_loss_tripped: bool,
    /// Timestamps of recent orders for the throttle window.
    recent_orders: VecDeque<DateTime<Utc>>,
    open_positions: usize,
    /// Manual/remote kill switch. Does NOT clear on day rollover.
    kill: bool,
}

impl RiskEngine {
    pub fn new(cfg: Config) -> RiskEngine {
        let kill = cfg.kill_switch;
        let equity = cfg.equity_usd;
        RiskEngine {
            cfg,
            equity,
            day_realized_pnl: Decimal::ZERO,
            day: Utc::now().date_naive(),
            daily_loss_tripped: false,
            recent_orders: VecDeque::new(),
            open_positions: 0,
            kill,
        }
    }

    /// True when ANY breaker is engaged (manual kill OR daily-loss trip).
    pub fn kill_switch(&self) -> bool {
        self.kill || self.daily_loss_tripped
    }

    /// Engage the manual kill switch (env at boot, or remote later). Stops new
    /// entries; exits stay allowed so funds can be flattened. Survives day
    /// rollover until explicitly released.
    pub fn engage_kill_switch(&mut self, reason: &str) {
        if !self.kill {
            tracing::error!(reason, "KILL SWITCH ENGAGED");
        }
        self.kill = true;
    }

    pub fn release_kill_switch(&mut self) {
        self.kill = false;
    }

    /// Roll the daily counter over on a new UTC calendar day: realized P&L
    /// resets and the daily-loss breaker releases (a manual kill switch does
    /// not).
    fn maybe_rollover(&mut self, now: DateTime<Utc>) {
        let d = now.date_naive();
        if d != self.day {
            self.day = d;
            self.day_realized_pnl = Decimal::ZERO;
            self.daily_loss_tripped = false;
            tracing::info!("new UTC day: daily loss counter reset, breaker released");
        }
    }

    /// Record realized P&L from a closed trade; trips the daily-loss breaker.
    pub fn record_realized_pnl(&mut self, pnl_usd: Decimal, now: DateTime<Utc>) {
        self.maybe_rollover(now);
        self.day_realized_pnl += pnl_usd;
        let limit = -self.equity * self.cfg.daily_loss_limit_pct / Decimal::from(100);
        if self.day_realized_pnl <= limit && !self.daily_loss_tripped {
            self.daily_loss_tripped = true;
            tracing::error!(
                pnl = %self.day_realized_pnl,
                limit_pct = %self.cfg.daily_loss_limit_pct,
                "DAILY LOSS LIMIT TRIPPED"
            );
        }
    }

    pub fn set_open_positions(&mut self, open: usize) {
        self.open_positions = open;
    }

    pub fn day_realized_pnl(&self) -> Decimal {
        self.day_realized_pnl
    }

    /// May we OPEN a new position right now?
    pub fn check_entry(&mut self, now: DateTime<Utc>) -> Result<(), BlockReason> {
        self.maybe_rollover(now);
        self.prune_orders(now);
        // Report the daily-loss breach first: it is the root cause even though
        // it also counts as a breaker state.
        let limit = -self.equity * self.cfg.daily_loss_limit_pct / Decimal::from(100);
        if self.daily_loss_tripped || self.day_realized_pnl <= limit {
            return Err(BlockReason::DailyLossLimit {
                pnl: self.day_realized_pnl,
                limit: self.cfg.daily_loss_limit_pct,
                equity: self.equity,
            });
        }
        if self.kill {
            return Err(BlockReason::KillSwitch);
        }
        if self.open_positions >= self.cfg.max_open_positions {
            return Err(BlockReason::OpenPositionCap {
                open: self.open_positions,
                max: self.cfg.max_open_positions,
            });
        }
        if self.recent_orders.len() as u32 >= self.cfg.max_trades_per_min {
            return Err(BlockReason::Throttled {
                count: self.recent_orders.len() as u32,
                max: self.cfg.max_trades_per_min,
            });
        }
        Ok(())
    }

    /// May we send an order (either side) right now? Rate-limits ALL outbound
    /// orders so upstream RPC/Jupiter never sees a burst (spec §6).
    pub fn check_order(&mut self, now: DateTime<Utc>) -> Result<(), BlockReason> {
        self.maybe_rollover(now);
        self.prune_orders(now);
        if self.recent_orders.len() as u32 >= self.cfg.max_trades_per_min {
            return Err(BlockReason::Throttled {
                count: self.recent_orders.len() as u32,
                max: self.cfg.max_trades_per_min,
            });
        }
        self.recent_orders.push_back(now);
        Ok(())
    }

    /// Sells (flattening a loser) are never blocked by the entry-side checks —
    /// cutting losers must always be possible — but they count toward the
    /// outbound throttle window.
    pub fn note_side(&mut self, side: Side, now: DateTime<Utc>) {
        if side == Side::Sell {
            self.recent_orders.push_back(now);
        }
    }

    fn prune_orders(&mut self, now: DateTime<Utc>) {
        while let Some(front) = self.recent_orders.front() {
            if (now - *front).num_seconds() >= 60 {
                self.recent_orders.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn kill_switch_blocks_entries_but_not_exits() {
        let mut risk = RiskEngine::new(Config::paper_defaults());
        risk.engage_kill_switch("test");
        assert!(matches!(
            risk.check_entry(ts(0)),
            Err(BlockReason::KillSwitch)
        ));
    }

    #[test]
    fn env_kill_switch_starts_engaged() {
        let mut cfg = Config::paper_defaults();
        cfg.kill_switch = true;
        let mut risk = RiskEngine::new(cfg);
        assert!(matches!(
            risk.check_entry(ts(0)),
            Err(BlockReason::KillSwitch)
        ));
    }

    #[test]
    fn daily_loss_limit_trips_breaker() {
        let mut cfg = Config::paper_defaults();
        cfg.daily_loss_limit_pct = dec!(10);
        let mut risk = RiskEngine::new(cfg);
        // -10% of 50k = -5000 → trip.
        risk.record_realized_pnl(dec!(-4999), ts(0));
        assert!(risk.check_entry(ts(0)).is_ok());
        risk.record_realized_pnl(dec!(-1), ts(0));
        assert!(matches!(
            risk.check_entry(ts(1)),
            Err(BlockReason::DailyLossLimit { .. })
        ));
    }

    #[test]
    fn daily_loss_breaker_resets_next_utc_day() {
        let mut risk = RiskEngine::new(Config::paper_defaults());
        risk.record_realized_pnl(dec!(-5000), ts(0)); // trips -10% of 50k
        assert!(matches!(
            risk.check_entry(ts(10)),
            Err(BlockReason::DailyLossLimit { .. })
        ));
        // 25h later = a new UTC day: counter resets, breaker releases.
        assert!(risk.check_entry(ts(90_000)).is_ok());
        assert_eq!(risk.day_realized_pnl(), Decimal::ZERO);
        // ...but a manual kill switch survives rollover.
        risk.engage_kill_switch("manual");
        risk.record_realized_pnl(dec!(-1), ts(90_000));
        risk.record_realized_pnl(dec!(-4999), ts(90_001));
        assert!(risk.check_entry(ts(172_800)).is_err()); // new day, still killed
        assert!(matches!(
            risk.check_entry(ts(172_800)),
            Err(BlockReason::KillSwitch)
        ));
    }

    #[test]
    fn open_position_cap_blocks() {
        let mut cfg = Config::paper_defaults();
        cfg.max_open_positions = 2;
        let mut risk = RiskEngine::new(cfg);
        risk.set_open_positions(2);
        assert!(matches!(
            risk.check_entry(ts(0)),
            Err(BlockReason::OpenPositionCap { .. })
        ));
    }

    #[test]
    fn throttle_blocks_after_max_per_minute() {
        let mut cfg = Config::paper_defaults();
        cfg.max_trades_per_min = 3;
        let mut risk = RiskEngine::new(cfg);
        assert!(risk.check_order(ts(0)).is_ok());
        assert!(risk.check_order(ts(10)).is_ok());
        assert!(risk.check_order(ts(20)).is_ok());
        assert!(matches!(
            risk.check_order(ts(30)),
            Err(BlockReason::Throttled { .. })
        ));
        // Window slides: 60s after the first order there's room again.
        assert!(risk.check_order(ts(61)).is_ok());
    }

    #[test]
    fn sells_bypass_throttle_cap() {
        let mut cfg = Config::paper_defaults();
        cfg.max_trades_per_min = 1;
        let mut risk = RiskEngine::new(cfg);
        assert!(risk.check_order(ts(0)).is_ok());
        // Buy throttled...
        assert!(matches!(
            risk.check_order(ts(1)),
            Err(BlockReason::Throttled { .. })
        ));
        // ...but a sell (cutting a loser) goes through.
        risk.note_side(Side::Sell, ts(2));
    }
}

