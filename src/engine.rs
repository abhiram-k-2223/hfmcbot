//! Engine: wires ingest → strategy → risk → execution → persistence (spec §1).
//! One state machine per token; identical code paths for paper and replay.

use crate::config::Config;
use crate::exec::Executor;
use crate::persist::{fill_record, AuditLog, AuditRecord};
use crate::risk::{BlockReason, RiskEngine};
use crate::strategy;
use crate::types::{
    ClosedTrade, Event, Fill, HoldMode, Launch, Launchpad, Position, PriceUpdate, Side,
};
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
    /// M6 operator alerting. `None` (all tests, default runs) = alert sites
    /// are compiled in but silent — `with_alerter` arms them in main.
    alerter: Option<crate::alerts::Alerter>,
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
            alerter: None,
        }
    }

    /// Arm M6 alerting (breaker trips, stuck positions, unknown exits).
    /// Builder-style so every existing `Engine::new` call site is untouched.
    pub fn with_alerter(mut self, alerter: crate::alerts::Alerter) -> Engine {
        self.alerter = Some(alerter);
        self
    }

    /// Fire-and-forget: logs + queues when armed, compiled-out no-op when
    /// not. Never blocks, never fails, never affects trading decisions.
    fn alert(&self, kind: crate::alerts::AlertKind, detail: String) {
        if let Some(a) = &self.alerter {
            a.fire(kind, detail);
        }
    }

    pub fn equity(&self) -> Decimal {
        self.equity
    }

    /// Capture the full resumable state (M6 crash-safe snapshots).
    pub fn state(&self) -> crate::persist::EngineState {
        let mut positions: Vec<Position> = self.positions.values().cloned().collect();
        positions.sort_by(|a, b| a.mint.cmp(&b.mint));
        crate::persist::EngineState {
            version: crate::persist::EngineState::VERSION,
            equity_usd: self.equity,
            deployed_usd: self.deployed_usd,
            positions,
            closed: self.closed.clone(),
            order_seq: self.order_seq,
            risk: self.risk.snapshot_state(),
        }
    }

    /// Write the current state atomically to `path` (see `persist::save_state`
    /// for the crash guarantee). Callers should invoke this periodically and
    /// on graceful shutdown.
    pub fn save_state(&self, path: &std::path::Path) -> std::io::Result<()> {
        crate::persist::save_state(path, &self.state())
    }

    /// Rebuild an engine from a snapshot. The restored book must satisfy the
    /// same money invariants as a live book — a snapshot that fails them is
    /// refused rather than traded on.
    pub fn restore(
        cfg: Config,
        exec: Box<dyn Executor>,
        audit: AuditLog,
        snap: crate::persist::EngineState,
    ) -> Result<Engine, String> {
        let mut eng = Engine::new(cfg, exec, audit);
        eng.equity = snap.equity_usd;
        eng.deployed_usd = snap.deployed_usd;
        eng.positions = snap
            .positions
            .into_iter()
            .map(|p| (p.mint.clone(), p))
            .collect();
        eng.closed = snap.closed;
        eng.order_seq = snap.order_seq;
        eng.risk.restore_state(snap.risk);
        eng.risk.set_open_positions(eng.positions.len());
        eng.check_invariants()?;
        Ok(eng)
    }

    pub fn deployed_usd(&self) -> Decimal {
        self.deployed_usd
    }

    pub fn open_positions(&self) -> usize {
        self.positions.len()
    }

    /// Sorted open mints — deterministic order for trade-subscription
    /// reconciliation (M5 price-path feed: subscribe trades only for held
    /// tokens, D13 fair-use). Sorted so sub/unsub plans are stable
    /// run-to-run given the same book.
    pub fn open_mints(&self) -> Vec<String> {
        let mut mints: Vec<String> = self.positions.keys().cloned().collect();
        mints.sort();
        mints
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

    pub async fn on_event(&mut self, ev: &Event) {
        let was_kill = self.risk.kill_switch();
        match ev {
            Event::Launch(l) => self.on_launch(l.clone()).await,
            Event::Price(p) => self.on_price_update(p.clone()).await,
        }
        // Time-stop sweep on EVERY event: a quiet/stale feed can never strand
        // a position past max_hold / conviction_max_hold.
        self.sweep_time_stops(ev.ts()).await;
        // Breaker transition (M6 alerting): the daily-loss trip engages
        // inside event handling — page once on the edge, not the level.
        if !was_kill && self.risk.kill_switch() {
            self.alert(
                crate::alerts::AlertKind::DailyLossTrip,
                format!(
                    "breaker engaged: day realized P&L ${}",
                    self.day_realized_pnl()
                ),
            );
        }
    }

    /// New launch detected: entry gate → risk checks → risk-first sizing →
    /// funnel slices fired back-to-back (the ≤5s burst from REPORT.md).
    async fn on_launch(&mut self, launch: Launch) {
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
            self.audit_decision(
                now,
                &mint,
                "entry_rejected",
                "no free capital for a new position",
            );
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
                .buy(
                    &mint,
                    *slice,
                    launch.price_usd,
                    launch.liquidity_usd,
                    now,
                    deadline,
                    &order_id,
                )
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    // Buy-side UNKNOWN is the scariest failure in the system:
                    // the bundle may have landed (SOL left, tokens arrived)
                    // while the book records nothing. Page the operator —
                    // manual reconciliation beats silent divergence.
                    if matches!(e, crate::exec::ExecError::Transport(_)) {
                        self.alert(
                            crate::alerts::AlertKind::TransportUnknown,
                            format!("{mint}: buy {order_id} unknown state — {e}"),
                        );
                    }
                    self.audit_decision(now, &mint, "order_failed", &e.to_string());
                    break;
                }
            };
            self.apply_buy_fill(&audit_fill, launch.launchpad, launch.liquidity_usd, now);
            self.log_order_and_fill(
                now,
                &order_id,
                &mint,
                Side::Buy,
                *slice,
                launch.price_usd,
                &audit_fill,
            );
        }
    }

    fn next_order_id(&mut self, mint: &str) -> String {
        self.order_seq += 1;
        format!("{mint}#{}", self.order_seq)
    }

    /// Accumulate a funnel-slice fill into the per-token position (VWAP entry).
    fn apply_buy_fill(
        &mut self,
        fill: &Fill,
        launchpad: Launchpad,
        liquidity_usd: Decimal,
        _now: DateTime<Utc>,
    ) {
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
            last_liquidity_usd: liquidity_usd,
            mode: HoldMode::Flip,
            exit_attempts: 0,
            stuck: false,
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
        pos.last_liquidity_usd = liquidity_usd;
        if fill.price_usd > pos.high_water {
            pos.high_water = fill.price_usd;
        }
        self.deployed_usd += cost;
        self.risk.set_open_positions(self.positions.len());
    }

    /// Flatten positions whose time stop elapsed while their feed went quiet.
    /// Uses `last_price` / `last_liquidity_usd` as the sell reference — the
    /// best information we have without a fresh tick.
    async fn sweep_time_stops(&mut self, now: DateTime<Utc>) {
        /// (mint, qty, cost, mode, entry, ref_price, liquidity, reason).
        type TimeStopExit = (
            String,
            Decimal,
            Decimal,
            HoldMode,
            Decimal,
            Decimal,
            Decimal,
            &'static str,
        );
        let mut exits: Vec<TimeStopExit> = Vec::new();
        for pos in self.positions.values_mut() {
            // Stuck positions are skipped here: re-firing an exit that already
            // demonstrated unfillability on every event would be blind
            // re-submission (spec §5.5). Fresh price ticks still retry them.
            if pos.stuck {
                continue;
            }
            if let Some(d) = strategy::on_time(pos, now, &self.cfg) {
                exits.push((
                    pos.mint.clone(),
                    pos.qty,
                    pos.cost_usd,
                    pos.mode,
                    pos.entry_price,
                    pos.last_price,
                    pos.last_liquidity_usd,
                    d.reason,
                ));
            }
        }
        for (mint, qty, cost, mode, entry, ref_price, liq, reason) in exits {
            self.close_position(&mint, qty, cost, mode, entry, ref_price, liq, now, reason)
                .await;
        }
    }

    /// Continuous price feed: drives exit rules (stop/TP/time/trail) and
    /// conviction promotion.
    async fn on_price_update(&mut self, pu: PriceUpdate) {
        let Some(pos) = self.positions.get_mut(&pu.mint) else {
            return;
        };
        pos.last_price = pu.price_usd;
        pos.last_liquidity_usd = pu.liquidity_usd;
        let Some(exit) = strategy::on_price(pos, pu.price_usd, pu.ts, &self.cfg) else {
            return;
        };
        let (mint, qty, cost, mode, entry_price) = (
            pos.mint.clone(),
            pos.qty,
            pos.cost_usd,
            pos.mode,
            pos.entry_price,
        );
        self.close_position(
            &mint,
            qty,
            cost,
            mode,
            entry_price,
            pu.price_usd,
            pu.liquidity_usd,
            pu.ts,
            exit.reason,
        )
        .await;
    }

    /// Flatten a position. Sells are NEVER blocked by risk checks (cutting
    /// losers must always be possible) — they only count toward the throttle.
    ///
    /// M3.5 reconciliation (spec §5.5): a failed sell keeps the position open
    /// and counts an exit attempt. Attempts keep coming on fresh price ticks
    /// (new information), but once `max_exit_attempts` is hit the position is
    /// marked `stuck` — still fully accounted in `deployed_usd` (the funds
    /// really are locked) and excluded from the time-stop sweep so the bot
    /// never blind-resubmits a demonstrated-unfillable exit on every event.
    #[allow(clippy::too_many_arguments)]
    async fn close_position(
        &mut self,
        mint: &str,
        qty: Decimal,
        cost: Decimal,
        mode: HoldMode,
        entry_price: Decimal,
        ref_price: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        reason: &str,
    ) {
        self.risk.note_side(Side::Sell, now);
        let order_id = self.next_order_id(mint);
        // M6 urgency tiers: flip stops outbid conviction trails (which
        // already banked outlier gains) — the live executor turns the tier
        // into a Jito tip + swap priority fee.
        let tier = match mode {
            HoldMode::Flip => crate::exec::TipTier::FlipExit,
            HoldMode::Conviction => crate::exec::TipTier::ConvictionExit,
        };
        let fill = match self
            .exec
            .sell(mint, qty, ref_price, liquidity_usd, now, &order_id, tier)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                // Scope the position borrow so the audits below can use
                // `&mut self` (disjoint field borrows, then borrow ends).
                let (attempts, newly_stuck, stuck_cost) =
                    if let Some(p) = self.positions.get_mut(mint) {
                        p.exit_attempts += 1;
                        let newly = !p.stuck && p.exit_attempts >= self.cfg.max_exit_attempts;
                        if newly {
                            p.stuck = true;
                        }
                        (p.exit_attempts, newly, p.cost_usd)
                    } else {
                        (0, false, Decimal::ZERO)
                    };
                if newly_stuck {
                    self.audit_decision(
                        now,
                        mint,
                        "position_stuck",
                        &format!(
                            "{attempts} failed exits (limit {}): treating as unsellable; \
                             ${stuck_cost} stays deployed, fresh ticks still retry",
                            self.cfg.max_exit_attempts,
                        ),
                    );
                    // Real money is immobilized on-chain — the operator must
                    // know even if they aren't watching the log.
                    self.alert(
                        crate::alerts::AlertKind::PositionStuck,
                        format!(
                            "{mint}: {attempts} failed exits, ${stuck_cost} stuck (mode {mode:?})"
                        ),
                    );
                }
                // Unknown order state (spec §5.5) pages every attempt: a
                // stuck-but-UNKNOWN exit may already have filled on-chain and
                // needs reconciliation, unlike a clean rejection.
                if matches!(e, crate::exec::ExecError::Transport(_)) {
                    self.alert(
                        crate::alerts::AlertKind::TransportUnknown,
                        format!("{mint} exit attempt {attempts}: {e}"),
                    );
                }
                self.audit_decision(
                    now,
                    mint,
                    "exit_failed",
                    &format!("attempt {attempts}: {e}"),
                );
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
        self.audit
            .append(&AuditRecord::TradeClosed(trade.clone()))
            .ok();
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
        let wins = self
            .closed
            .iter()
            .filter(|t| t.pnl_usd > Decimal::ZERO)
            .count();
        let stuck = self.positions.values().filter(|p| p.stuck).count();
        let total_pnl: Decimal = self.closed.iter().map(|t| t.pnl_usd).sum();
        let stuck_suffix = if stuck > 0 {
            format!(", STUCK {stuck}")
        } else {
            String::new()
        };
        format!(
            "equity ${} (start ${}), deployed ${}, open {}, closed {} ({}W/{}L), realized P&L ${}{}",
            self.equity,
            self.cfg.equity_usd,
            self.deployed_usd,
            self.open_positions(),
            self.closed.len(),
            wins,
            self.closed.len() - wins,
            total_pnl,
            stuck_suffix,
        )
    }

    /// Point-in-time numbers for the `/metrics` endpoint. Strings (not floats)
    /// so Prometheus sees exact Decimal values.
    pub fn snapshot(&self) -> crate::metrics::EngineSnapshot {
        let wins = self
            .closed
            .iter()
            .filter(|t| t.pnl_usd > Decimal::ZERO)
            .count();
        let total_pnl: Decimal = self.closed.iter().map(|t| t.pnl_usd).sum();
        crate::metrics::EngineSnapshot {
            equity_usd: self.equity.to_string(),
            deployed_usd: self.deployed_usd.to_string(),
            open_positions: self.open_positions(),
            closed_trades: self.closed.len(),
            wins,
            losses: self.closed.len() - wins,
            realized_pnl_usd: total_pnl.to_string(),
            day_realized_pnl_usd: self.day_realized_pnl().to_string(),
            kill_switch: self.kill_switch(),
            stuck_positions: self.positions.values().filter(|p| p.stuck).count(),
        }
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

    #[tokio::test]
    async fn funnel_entry_builds_position_across_slices() {
        let dir = tempfile::tempdir().unwrap();
        // risk_per_trade_pct 2.5% of 50k = 1250 → 3 slices of ~416.66.
        let mut cfg = Config::paper_defaults();
        cfg.funnel_slices = 3;
        let mut eng = engine(&dir, &cfg);

        eng.on_launch(launch("AAA", dec!(0.001))).await;
        assert_eq!(eng.open_positions(), 1);
        // Slices sum to the budget exactly; each slice pays a 1% fee (small
        // tolerance for Decimal division rounding on qty).
        assert!(eng.deployed_usd > dec!(1262) && eng.deployed_usd < dec!(1263));
        assert_eq!(eng.equity(), dec!(50000)); // unrealized: equity unchanged
    }

    #[tokio::test]
    async fn stop_loss_cuts_loser_and_daily_breaker_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::paper_defaults();
        cfg.stop_loss_pct = dec!(30);
        cfg.daily_loss_limit_pct = dec!(1); // -500 trips after two ~-310 losses
        let mut eng = engine(&dir, &cfg);

        // Two losers: entry 0.001, dump to 0.0006 (-40%) → stop_loss each.
        // Each loss ≈ -$549 on the $1250 budget; the -1% (-$500) daily limit
        // trips on the very first one, but the already-open L2 still gets cut
        // (sells are never blocked).
        eng.on_launch(launch("L1", dec!(0.001))).await;
        eng.on_launch(launch("L2", dec!(0.001))).await;
        eng.on_event(&price("L1", 10, dec!(0.0006))).await;
        eng.on_event(&price("L2", 11, dec!(0.0006))).await;
        assert_eq!(eng.closed_trades().len(), 2);
        assert!(eng.closed_trades()[0].pnl_usd < Decimal::ZERO);
        assert_eq!(eng.closed_trades()[0].exit_reason, "stop_loss");

        // Daily-loss breaker tripped...
        assert!(eng.kill_switch(), "daily loss breaker should have tripped");

        // A third launch must be blocked by the breaker.
        eng.on_launch(launch("L3", dec!(0.001))).await;
        assert_eq!(eng.open_positions(), 0);
    }

    #[tokio::test]
    async fn winner_is_promoted_and_trails_out() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());

        eng.on_launch(launch("ANSEM", dec!(0.001))).await;

        // +900% at the first tick → promoted to conviction (trail) instead of
        // the flip TP that would have capped the winner.
        eng.on_event(&price("ANSEM", 60, dec!(0.010))).await;
        assert_eq!(eng.open_positions(), 1);

        // Keep climbing; stays in.
        eng.on_event(&price("ANSEM", 120, dec!(0.440))).await;

        // 30% off the high (0.44 → 0.30) → trail_stop exits near the top.
        eng.on_event(&price("ANSEM", 130, dec!(0.299))).await;
        assert_eq!(eng.open_positions(), 0);
        let t = &eng.closed_trades()[0];
        assert_eq!(t.exit_reason, "trail_stop");
        assert!(t.pnl_usd > Decimal::ZERO, "winner must realize a profit");
    }

    #[tokio::test]
    async fn sweep_cuts_position_whose_feed_went_quiet() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());

        // STALE enters at ts(0) at price 1, then its feed NEVER ticks again.
        eng.on_launch(launch("STALE", dec!(1))).await;
        assert_eq!(eng.open_positions(), 1);

        // 7h later, a completely unrelated token's event arrives via the feed
        // entry point (on_event). The sweep must flatten STALE on the clock
        // alone (max_hold 6h), using last fill price as the sell reference.
        eng.on_event(&Event::Launch(launch_at("OTHER", 25_200, dec!(1))))
            .await;
        assert_eq!(eng.closed_trades().len(), 1);
        let t = &eng.closed_trades()[0];
        assert_eq!(t.mint, "STALE");
        assert_eq!(t.exit_reason, "max_hold");
    }

    #[tokio::test]
    async fn max_hold_closes_stale_flip() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());
        eng.on_launch(launch("STALE", dec!(1))).await;
        // Sideways for 6h+ → time stop.
        eng.on_event(&price("STALE", 21_700, dec!(1.01))).await;
        assert_eq!(eng.open_positions(), 0);
        assert_eq!(eng.closed_trades()[0].exit_reason, "max_hold");
    }

    /// Buys fill, every sell fails: after `max_exit_attempts` the position is
    /// marked stuck (funds stay deployed, equity untouched), the time-stop
    /// sweep skips it, but fresh price ticks still retry the exit.
    struct FailSells {
        inner: PaperExecutor,
    }

    #[async_trait::async_trait]
    impl crate::exec::Executor for FailSells {
        async fn buy(
            &mut self,
            mint: &str,
            budget_usd: Decimal,
            price_usd: Decimal,
            liquidity_usd: Decimal,
            now: DateTime<Utc>,
            deadline: DateTime<Utc>,
            order_id: &str,
        ) -> Result<Fill, crate::exec::ExecError> {
            self.inner
                .buy(
                    mint,
                    budget_usd,
                    price_usd,
                    liquidity_usd,
                    now,
                    deadline,
                    order_id,
                )
                .await
        }

        async fn sell(
            &mut self,
            _mint: &str,
            _qty: Decimal,
            _price_usd: Decimal,
            _liquidity_usd: Decimal,
            _now: DateTime<Utc>,
            order_id: &str,
            _tier: crate::exec::TipTier,
        ) -> Result<Fill, crate::exec::ExecError> {
            Err(crate::exec::ExecError::Rejected(format!(
                "simulated dead pool (order {order_id})"
            )))
        }
    }

    #[tokio::test]
    async fn persistently_failing_exits_mark_position_stuck() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::paper_defaults();
        cfg.max_exit_attempts = 2;
        cfg.max_hold_secs = 10; // time-stop eligible almost immediately
        let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
        let mut eng = Engine::new(
            cfg.clone(),
            Box::new(FailSells {
                inner: PaperExecutor::new(&cfg),
            }),
            audit,
        );

        eng.on_launch(launch("STUCK", dec!(0.001))).await;
        assert_eq!(eng.open_positions(), 1);

        // t=10: -40% tick → stop exit attempt #1 fails; the sweep then fires
        // the elapsed time-stop → attempt #2 fails → position marked stuck.
        eng.on_event(&price("STUCK", 10, dec!(0.0006))).await;
        {
            let p = &eng.positions["STUCK"];
            assert_eq!(p.exit_attempts, 2);
            assert!(p.stuck, "two failed exits must mark the position stuck");
        }
        assert_eq!(eng.open_positions(), 1);
        assert_eq!(eng.equity(), dec!(50000)); // nothing realized
        assert!(eng.deployed_usd > Decimal::ZERO); // funds stay deployed

        // Unrelated launch runs the sweep: the stuck position must be skipped
        // (no blind re-submission), while the fresh launch still enters.
        eng.on_event(&Event::Launch(launch_at("OTHER", 12, dec!(1))))
            .await;
        assert_eq!(eng.positions["STUCK"].exit_attempts, 2);
        assert_eq!(eng.open_positions(), 2);

        // A fresh price tick on STUCK still retries the exit (new information).
        eng.on_event(&price("STUCK", 13, dec!(0.0006))).await;
        assert_eq!(eng.positions["STUCK"].exit_attempts, 3);
        assert_eq!(eng.open_positions(), 2);

        let s = eng.snapshot();
        assert_eq!(s.stuck_positions, 1);
        let text = crate::metrics::render_prometheus_text(&s);
        assert!(text.contains("hfmcbot_stuck_positions 1"));
        assert!(eng.summary().contains("STUCK 1"));
        eng.check_invariants().unwrap();
    }

    #[tokio::test]
    async fn snapshot_feeds_metrics_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut eng = engine(&dir, &Config::paper_defaults());
        eng.on_launch(launch("AAA", dec!(0.001))).await;
        let s = eng.snapshot();
        assert_eq!(s.open_positions, 1);
        assert_eq!(s.closed_trades, 0);
        assert_eq!(s.wins, 0);
        assert!(!s.kill_switch);
        let text = crate::metrics::render_prometheus_text(&s);
        assert!(text.contains("hfmcbot_open_positions 1"));
    }

    #[tokio::test]
    async fn state_roundtrip_resumes_identical_book() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::paper_defaults();
        let mut eng = engine(&dir, &cfg);
        eng.on_launch(launch("AAA", dec!(0.001))).await;
        eng.on_launch(launch("BBB", dec!(0.002))).await;
        // Lethal tick on AAA closes it: exercises closed-trade + order_seq
        // carry-over, not just open positions.
        eng.on_event(&price("AAA", 10, dec!(0.00001))).await;
        assert_eq!(eng.open_positions(), 1);
        assert_eq!(eng.closed.len(), 1);
        eng.check_invariants().unwrap();

        let snap_path = dir.path().join("state.json");
        eng.save_state(&snap_path).unwrap();

        // Restore into a FRESH engine (new audit log, same config).
        let audit2 = AuditLog::open(&dir.path().join("audit2.jsonl")).unwrap();
        let snap = crate::persist::load_state(&snap_path).unwrap();
        let eng2 = Engine::restore(
            cfg.clone(),
            Box::new(PaperExecutor::new(&cfg)),
            audit2,
            snap,
        )
        .unwrap();

        assert_eq!(eng2.open_mints(), eng.open_mints());
        assert_eq!(eng2.equity(), eng.equity());
        assert_eq!(eng2.deployed_usd, eng.deployed_usd);
        assert_eq!(eng2.closed.len(), eng.closed.len());
        assert_eq!(eng2.order_seq, eng.order_seq);
        assert_eq!(
            eng2.snapshot().realized_pnl_usd,
            eng.snapshot().realized_pnl_usd
        );
        eng2.check_invariants().unwrap();

        // The restored engine keeps trading identically: a tick on the held
        // position is processed, not dropped.
        let mut eng2 = eng2;
        eng2.on_event(&price("BBB", 20, dec!(0.002))).await;
        eng2.check_invariants().unwrap();
    }

    #[tokio::test]
    async fn restore_keeps_kill_engaged_and_rejects_bad_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        // Boot with the kill switch on, snapshot, then restore into an engine
        // whose config has it OFF: the kill must survive (OR, never clear).
        let mut cfg = Config::paper_defaults();
        cfg.kill_switch = true;
        let eng = engine(&dir, &cfg);
        assert!(eng.kill_switch());
        let snap_path = dir.path().join("state.json");
        eng.save_state(&snap_path).unwrap();

        let cfg2 = Config::paper_defaults();
        let audit2 = AuditLog::open(&dir.path().join("audit2.jsonl")).unwrap();
        let snap = crate::persist::load_state(&snap_path).unwrap();
        let eng2 = Engine::restore(
            cfg2.clone(),
            Box::new(PaperExecutor::new(&cfg2)),
            audit2,
            snap,
        )
        .unwrap();
        assert!(eng2.kill_switch(), "restored kill must stay engaged");

        // Garbage and wrong-version files are refused loudly.
        let bad = dir.path().join("bad.json");
        std::fs::write(&bad, "not json").unwrap();
        assert!(crate::persist::load_state(&bad).is_err());
        let mut v9: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&snap_path).unwrap()).unwrap();
        v9["version"] = serde_json::json!(999);
        let badv = dir.path().join("badv.json");
        std::fs::write(&badv, serde_json::to_string(&v9).unwrap()).unwrap();
        assert!(crate::persist::load_state(&badv).is_err());
    }

    /// M6: a tripped breaker pages exactly once (edge, not level), and a
    /// stuck position pages with the immobilized amount.
    #[tokio::test]
    async fn alerts_fire_on_breaker_trip_and_stuck() {
        use crate::alerts::{AlertKind, Alerter};

        // Daily-loss trip: 1% of 50k = -$500 — one ~total-loss close trips.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::paper_defaults();
        cfg.daily_loss_limit_pct = dec!(1);
        let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
        let (alerter, rx) = Alerter::new(true, 300);
        let mut rx = rx.expect("webhook mode yields a receiver");
        let mut eng = Engine::new(cfg.clone(), Box::new(PaperExecutor::new(&cfg)), audit)
            .with_alerter(alerter);
        eng.on_launch(launch("A1", dec!(0.001))).await;
        eng.on_launch(launch("A2", dec!(0.001))).await;
        assert_eq!(eng.open_positions(), 2);
        // -99.99% ticks close both at ~total loss (≈ -$1250 each) → trip on
        // the first close; the second close must NOT re-page (edge, level).
        eng.on_event(&price("A1", 10, dec!(0.0000001))).await;
        eng.on_event(&price("A2", 11, dec!(0.0000001))).await;
        assert!(eng.kill_switch());

        let first = rx.try_recv().expect("breaker trip must alert");
        assert_eq!(first.kind, AlertKind::DailyLossTrip);
        assert!(rx.try_recv().is_err(), "no repeat page on level");

        // Stuck: FailSells wrapper fails every exit; max_exit_attempts = 2.
        let dir2 = tempfile::tempdir().unwrap();
        let mut cfg2 = Config::paper_defaults();
        cfg2.max_exit_attempts = 2;
        let audit2 = AuditLog::open(&dir2.path().join("audit.jsonl")).unwrap();
        let (alerter2, rx2) = Alerter::new(true, 300);
        let mut rx2 = rx2.expect("webhook mode yields a receiver");
        let mut eng2 = Engine::new(
            cfg2.clone(),
            Box::new(FailSells {
                inner: PaperExecutor::new(&cfg2),
            }),
            audit2,
        )
        .with_alerter(alerter2);
        eng2.on_launch(launch("STUCK", dec!(0.001))).await;
        // Two lethal ticks → two failed stop exits → stuck at
        // max_exit_attempts = 2.
        eng2.on_event(&price("STUCK", 10, dec!(0.0006))).await;
        eng2.on_event(&price("STUCK", 11, dec!(0.0006))).await;
        assert!(eng2.positions["STUCK"].stuck);
        let stuck = rx2.try_recv().expect("stuck position must alert");
        assert_eq!(stuck.kind, AlertKind::PositionStuck);
        assert!(stuck.detail.contains("STUCK"));
    }
}
