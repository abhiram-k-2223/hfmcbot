//! Engine: wires ingest → strategy → risk → execution → persistence (spec §1).
//! One state machine per token; identical code paths for paper and replay.

use crate::config::Config;
use crate::exec::Executor;
use crate::persist::{fill_record, AuditLog, AuditRecord};
use crate::risk::{BlockReason, RiskEngine};
use crate::strategy;
use crate::types::{ClosedTrade, Event, Fill, HoldMode, Launch, Launchpad, Position, PriceUpdate, Side};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

pub struct Engine {
    cfg: Config,
    risk: RiskEngine,
    exec: Box<dyn Executor>,
    audit: AuditLog,
    /// Realized equity (starting equity + closed P&L).
    equity: Decimal,
    /// USD currently locked in open positions (cost incl. fees).
    deployed_usd: Decimal,
    positions: HashMap<String, Position>,
    closed: Vec<ClosedTrade>,
    order_seq: u64,
}

impl Engine {
    pub fn new(cfg: Config, exec: Box<dyn Executor>, audit: AuditLog) -> Engine {
        let equity = cfg.equity_usd;
        Engine {
            risk: RiskEngine::new(cfg.clone()),
            cfg,
            exec,
            audit,
            equity,
            deployed_usd: Decimal::ZERO,
            positions: HashMap::new(),
            closed: Vec::new(),
            order_seq: 0,
        }
    }

    pub fn equity(&self) -> Decimal {
        self.equity
    }

    pub fn deployed_usd(&self) -> Decimal {
        self.deployed_usd
    }

    pub fn open_positions(&self) -> usize {
        self.positions.len()
    }

    pub fn closed_trades(&self) -> &[ClosedTrade] {
        &self.closed
    }

    pub fn kill_switch(&self) -> bool {
        self.risk.kill_switch()
    }

    pub fn day_realized_pnl(&self) -> Decimal {
        self.risk.day_realized_pnl()
    }

    pub fn on_event(&mut self, ev: &Event) {
        match ev {
            Event::Launch(l) => self.on_launch(l.clone()),
            Event::Price(p) => self.on_price_update(p.clone()),
        }
        // Time-stop sweep on EVERY event: a quiet/stale feed can never strand
        // a position past max_hold / conviction_max_hold.
        self.sweep_time_stops(ev.ts());
    }

    /// New launch detected: entry gate → risk checks → risk-first sizing →
    /// funnel slices fired back-to-back (the ≤5s burst from REPORT.md).
    fn on_launch(&mut self, launch: Launch) {
        let now = launch.created_at;
        let mint = launch.mint.clone();

        if self.positions.contains_key(&mint) {
            self.audit_decision(now, &mint, "entry", "already holding; skipping re-entry");
            return;
        }

        if let Err(reason) = strategy::entry_gate(&launch, now, &self.cfg) {
            self.audit_decision(now, &mint, "entry_rejected", &reason.to_string());
            return;
        }

        if let Err(block) = self.risk.check_entry(now) {
            self.audit_blocked(now, &mint, &block);
            return;
        }

        let budget = strategy::position_budget(&self.cfg, self.equity, self.deployed_usd);
        if budget <= Decimal::ZERO {
            self.audit_decision(now, &mint, "entry_rejected", "no free capital for a new position");
            return;
        }

        self.audit_decision(
            now,
            &mint,
            "entry_accepted",
            &format!("funnel budget ${budget} into {}", launch.launchpad.as_str()),
        );

        // Funnel: send all slices back-to-back within the configured window.
        // The deadline is part of the executor contract — a live (async)
        // executor must refuse slices past it; the throttle may also cut the
        // funnel short, leaving a partially-filled smaller position.
        let deadline = now + Duration::seconds(self.cfg.funnel_window_secs as i64);
        let slices = strategy::split_into_slices(budget, self.cfg.funnel_slices);
        for (idx, slice) in slices.iter().enumerate() {
            if now > deadline {
                self.audit_decision(
                    now,
                    &mint,
                    "funnel_window_expired",
                    &format!("slice {}/{} not sent", idx + 1, slices.len()),
                );
                break;
            }
            if let Err(block) = self.risk.check_order(now) {
                self.audit_blocked(now, &mint, &block);
                self.audit_decision(
                    now,
                    &mint,
                    "funnel_aborted",
                    &format!("slices {}/{} unfilled", idx + 1, slices.len()),
                );
                break;
            }
            let order_id = self.next_order_id(&mint);
            let audit_fill = match self
                .exec
                .buy(&mint, *slice, launch.price_usd, now, deadline, &order_id)
            {
                Ok(f) => f,
                Err(e) => {
                    self.audit_decision(now, &mint, "order_failed", &e.to_string());
                    break;
                }
            };
            self.apply_buy_fill(&audit_fill, launch.launchpad, now);
            self.log_order_and_fill(now, &order_id, &mint, Side::Buy, *slice, launch.price_usd, &audit_fill);
        }
    }

    fn next_order_id(&mut self, mint: &str) -> String {
        self.order_seq += 1;
        format!("{mint}#{}", self.order_seq)
    }

    /// Accumulate a funnel-slice fill into the per-token position (VWAP entry).
    fn apply_buy_fill(&mut self, fill: &Fill, launchpad: Launchpad, _now: DateTime<Utc>) {
        let cost = fill.notional_usd + fill.fee_usd;
        let pos = self.positions.entry(fill.mint.clone()).or_insert(Position {
            mint: fill.mint.clone(),
            launchpad,
            opened_at: fill.ts,
            entry_price: fill.price_usd,
            qty: Decimal::ZERO,
            cost_usd: Decimal::ZERO,
            high_water: fill.price_usd,
            last_price: fill.price_usd,
            mode: HoldMode::Flip,
        });
        let new_qty = pos.qty + fill.qty;
        // Qty-weighted average entry across slices (fee-free basis for the
        // % gates; fees are tracked in cost_usd for P&L).
        if new_qty > Decimal::ZERO {
            pos.entry_price = (pos.entry_price * pos.qty + fill.price_usd * fill.qty) / new_qty;
        }
        pos.qty = new_qty;
        pos.cost_usd += cost;
        pos.last_price = fill.price_usd;
        if fill.price_usd > pos.high_water {
            pos.high_water = fill.price_usd;
        }
        self.deployed_usd += cost;
        self.risk.set_open_positions(self.positions.len());
    }

    /// Flatten positions whose time stop elapsed while their feed went quiet.
    /// Uses `last_price` as the sell reference — the best information we have
    /// without a fresh tick.
    fn sweep_time_stops(&mut self, now: DateTime<Utc>) {
        let mut exits: Vec<(String, Decimal, Decimal, HoldMode, Decimal, Decimal, &'static str)> =
            Vec::new();
        for pos in self.positions.values_mut() {
            if let Some(d) = strategy::on_time(pos, now, &self.cfg) {
                exits.push((
                    pos.mint.clone(),
                    pos.qty,
                    pos.cost_usd,
                    pos.mode,
                    pos.entry_price,
                    pos.last_price,
                    d.reason,
                ));
            }
        }
        for (mint, qty, cost, mode, entry, ref_price, reason) in exits {
            self.close_position(&mint, qty, cost, mode, entry, ref_price, now, reason);
        }
    }

    /// Continuous price feed: drives exit rules (stop/TP/time/trail) and
    /// conviction promotion.
    fn on_price_update(&mut self, pu: PriceUpdate) {
        let Some(pos) = self.positions.get_mut(&pu.mint) else {
            return;
        };
        pos.last_price = pu.price_usd;
        let Some(exit) = strategy::on_price(pos, pu.price_usd, pu.ts, &self.cfg) else {
            return;
        };
        let (mint, qty, cost, mode, entry_price) =
            (pos.mint.clone(), pos.qty, pos.cost_usd, pos.mode, pos.entry_price);
        self.close_position(
            &mint, qty, cost, mode, entry_price, pu.price_usd, pu.ts, exit.reason,
        );
    }

    /// Flatten a position. Sells are NEVER blocked by risk checks (cutting
    /// losers must always be possible) — they only count toward the throttle.
    fn close_position(
        &mut self,
        mint: &str,
        qty: Decimal,
        cost: Decimal,
        mode: HoldMode,
        entry_price: Decimal,
        ref_price: Decimal,
        now: DateTime<Utc>,
        reason: &str,
    ) {
        self.risk.note_side(Side::Sell, now);
        let order_id = self.next_order_id(mint);
        let fill = match self.exec.sell(mint, qty, ref_price, now, &order_id) {
            Ok(f) => f,
            Err(e) => {
                self.audit_decision(now, mint, "exit_failed", &e.to_string());
                return;
            }
        };
        let proceeds = (fill.notional_usd - fill.fee_usd).round_dp(crate::exec::FILL_DP);
        let pnl = proceeds - cost;
        let pnl_pct = if cost.is_zero() {
            Decimal::ZERO
        } else {
            (pnl / cost * Decimal::from(100)).round_dp(crate::exec::FILL_DP)
        };

        let was_kill = self.risk.kill_switch();
        self.equity += pnl;
        self.deployed_usd -= cost;
        self.risk.record_realized_pnl(pnl, now);
        if !was_kill && self.risk.kill_switch() {
            self.audit
                .append(&AuditRecord::Breaker {
                    ts: now,
                    reason: "daily loss limit tripped".into(),
                })
                .ok();
        }
        self.positions.remove(mint);
        self.risk.set_open_positions(self.positions.len());

        let trade = ClosedTrade {
            mint: mint.to_string(),
            opened_at: fill.ts,
            closed_at: now,
            entry_price,
            exit_price: fill.price_usd,
            qty: fill.qty,
            cost_usd: cost,
            proceeds_usd: proceeds,
            pnl_usd: pnl,
            pnl_pct,
            exit_reason: reason.to_string(),
            mode,
        };
        self.audit
            .append(&AuditRecord::Order {
                ts: now,
                order_id: order_id.clone(),
                mint: mint.to_string(),
                side: "sell".into(),
                budget_or_qty: qty,
                ref_price,
            })
            .ok();
        self.audit.append(&fill_record(&fill)).ok();
        self.audit.append(&AuditRecord::TradeClosed(trade.clone())).ok();
        tracing::info!(
            mint,
            reason,
            pnl_usd = %pnl,
            pnl_pct = %pnl_pct,
            equity = %self.equity,
            "position closed"
        );
        self.closed.push(trade);
    }

    fn audit_decision(&mut self, now: DateTime<Utc>, mint: &str, action: &str, detail: &str) {
        tracing::debug!(mint, action, detail, "decision");
        self.audit
            .append(&AuditRecord::Decision {
                ts: now,
                mint: mint.to_string(),
                action: action.to_string(),
                detail: detail.to_string(),
            })
            .ok();
    }

    fn audit_blocked(&mut self, now: DateTime<Utc>, mint: &str, block: &BlockReason) {
        tracing::warn!(mint, block = %block, "entry blocked");
        self.audit
            .append(&AuditRecord::Decision {
                ts: now,
                mint: mint.to_string(),
                action: "entry_blocked".to_string(),
                detail: block.to_string(),
            })
            .ok();
    }

    #[allow(clippy::too_many_arguments)]
    fn log_order_and_fill(
        &mut self,
        now: DateTime<Utc>,
        order_id: &str,
        mint: &str,
        side: Side,
        budget_or_qty: Decimal,
        ref_price: Decimal,
        fill: &Fill,
    ) {
        self.audit
            .append(&AuditRecord::Order {
                ts: now,
                order_id: order_id.to_string(),
                mint: mint.to_string(),
                side: format!("{side:?}").to_lowercase(),
                budget_or_qty,
                ref_price,
            })
            .ok();
        self.audit.append(&fill_record(fill)).ok();
    }

    /// Internal-consistency audit: P&L conservation and deployed-capital
    /// reconciliation. Every closed trade's P&L must equal proceeds − cost,
    /// equity must equal start equity + Σ realized P&L, and deployed capital
    /// must equal the sum of open-position costs. Called by tests/chaos runs
    /// after arbitrary event streams, regardless of which orders failed.
    pub fn check_invariants(&self) -> Result<(), String> {
        let closed_pnl: Decimal = self.closed.iter().map(|t| t.pnl_usd).sum();
        if self.equity != self.cfg.equity_usd + closed_pnl {
            return Err(format!(
                "equity ${} != start ${} + realized ${}",
                self.equity, self.cfg.equity_usd, closed_pnl
            ));
        }
        let open_cost: Decimal = self.positions.values().map(|p| p.cost_usd).sum();
        if self.deployed_usd != open_cost {
            return Err(format!(
                "deployed ${} != Σ open position cost ${}",
                self.deployed_usd, open_cost
            ));
        }
        for p in self.positions.values() {
            if p.qty <= Decimal::ZERO || p.cost_usd <= Decimal::ZERO {
                return Err(format!(
                    "open position {} has qty {} / cost ${}",
                    p.mint, p.qty, p.cost_usd
                ));
            }
        }
        for t in &self.closed {
            if t.pnl_usd != t.proceeds_usd - t.cost_usd {
                return Err(format!(
                    "closed trade {}: pnl ${} != proceeds ${} − cost ${}",
                    t.mint, t.pnl_usd, t.proceeds_usd, t.cost_usd
                ));
            }
        }
        Ok(())
    }

    /// One-line human summary for CLI output.
    pub fn summary(&self) -> String {
        let wins = self.closed.iter().filter(|t| t.pnl_usd > Decimal::ZERO).count();
        let total_pnl: Decimal = self.closed.iter().map(|t| t.pnl_usd).sum();
        format!(
            "equity ${} (start ${}), deployed ${}, open {}, closed {} ({}W/{}L), realized P&L ${}",
            self.equity,
            self.cfg.equity_usd,
            self.deployed_usd,
            self.open_positions(),
            self.closed.len(),
            wins,
            self.closed.len() - wins,
            total_pnl,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::exec::PaperExecutor;
    use crate::persist::AuditLog;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + s, 0).unwrap()
    }

    fn launch(mint: &str, price: Decimal) -> Launch {
        launch_at(mint, 0, price)
    }

    fn launch_at(mint: &str, at: i64, price: Decimal) -> Launch {
        Launch {
            mint: mint.into(),
            launchpad: Launchpad::PumpFun,
            created_at: ts(at),
            creator_hold_pct: dec!(1),
            mint_renounced: true,
            is_honeypot: false,
            liquidity_usd: dec!(8000),
            on_curve: true,
            price_usd: price,
        }
    }

    fn price(mint: &str, at: i64, p: Decimal) -> Event {
        Event::Price(PriceUpdate {
            mint: mint.into(),
            ts: ts(at),
            price_usd: p,
            liquidity_usd: dec!(8000),
        })
    }

    fn engine(dir: &tempfile::TempDir, cfg: &Config) -> Engine {
        let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
        Engine::new(cfg.clone(), Box::new(PaperExecutor::new(cfg)), audit)
    }

    #[test]
    fn funnel_entry_builds_position_across_slices() {
        let dir = tempfile::tempdir().unwrap();
        // risk_per_trade_pct 2.5% of 50k = 1250 → 3 slices of ~416.66.
        let mut cfg = Config::paper_defaults();
        cfg.funnel_slices = 3;
        let mut eng = engine(&dir, &cfg);

        eng.on_launch(launch("AAA", dec!(0.001)));
        assert_eq!(eng.open_positions(), 1);
        // Slices sum to the budget exactly; each slice pays a 1% fee (small
        // tolerance for Decimal division rounding on qty).
        assert!(eng.deployed_usd > dec!(1262) && eng.deployed_usd < dec!(1263));
        assert_eq!(eng.equity(), dec!(50000)); // unrealized: equity unchanged
    }

    #[test]
    fn stop_loss_cuts_loser_and_daily_breaker_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::paper_defaults();
        cfg.stop_loss_pct = dec!(30);
        cfg.daily_loss_limit_pct = dec!(1); // -500 trips after two ~-310 losses
        let mut eng = engine(&dir, &cfg);

        // Two losers: entry 0.001, dump to 0.0006 (-40%) → stop_loss each.
        // Each loss ≈ -$549 on the $1250 budget; the -1% (-$500) daily limit
        // trips on the very first one, but the already-open L2 still gets cut
        // (sells are never blocked).
        eng.on_launch(launch("L1", dec!(0.001)));
        eng.on_launch(launch("L2", dec!(0.001)));
        eng.on_event(&price("L1", 10, dec!(0.0006)));
        eng.on_event(&price("L2", 11, dec!(0.0006)));
        assert_eq!(eng.closed_trades().len(), 2);
        assert!(eng.closed_trades()[0].pnl_usd < Decimal::ZERO);
        assert_eq!(eng.closed_trades()[0].exit_reason, "stop_loss");

        // Daily-loss breaker tripped...
        assert!(eng.kill_switch(), "daily loss breaker should have tripped");

        // A third launch must be blocked by the breaker.
        eng.on_launch(launch("L3", dec!(0.001)));
        assert_eq!(eng.open_positions(), 0);
    }

    #[test]
    fn winner_is_promoted_and_trails_out() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());

        eng.on_launch(launch("ANSEM", dec!(0.001)));

        // +900% at the first tick → promoted to conviction (trail) instead of
        // the flip TP that would have capped the winner.
        eng.on_event(&price("ANSEM", 60, dec!(0.010)));
        assert_eq!(eng.open_positions(), 1);

        // Keep climbing; stays in.
        eng.on_event(&price("ANSEM", 120, dec!(0.440)));

        // 30% off the high (0.44 → 0.30) → trail_stop exits near the top.
        eng.on_event(&price("ANSEM", 130, dec!(0.299)));
        assert_eq!(eng.open_positions(), 0);
        let t = &eng.closed_trades()[0];
        assert_eq!(t.exit_reason, "trail_stop");
        assert!(t.pnl_usd > Decimal::ZERO, "winner must realize a profit");
    }

    #[test]
    fn sweep_cuts_position_whose_feed_went_quiet() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());

        // STALE enters at ts(0) at price 1, then its feed NEVER ticks again.
        eng.on_launch(launch("STALE", dec!(1)));
        assert_eq!(eng.open_positions(), 1);

        // 7h later, a completely unrelated token's event arrives via the feed
        // entry point (on_event). The sweep must flatten STALE on the clock
        // alone (max_hold 6h), using last fill price as the sell reference.
        eng.on_event(&Event::Launch(launch_at("OTHER", 25_200, dec!(1))));
        assert_eq!(eng.closed_trades().len(), 1);
        let t = &eng.closed_trades()[0];
        assert_eq!(t.mint, "STALE");
        assert_eq!(t.exit_reason, "max_hold");
    }

    #[test]
    fn max_hold_closes_stale_flip() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());
        eng.on_launch(launch("STALE", dec!(1)));
        // Sideways for 6h+ → time stop.
        eng.on_event(&price("STALE", 21_700, dec!(1.01)));
        assert_eq!(eng.open_positions(), 0);
        assert_eq!(eng.closed_trades()[0].exit_reason, "max_hold");
    }
}

