//! Execution module (spec §5). The `Executor` trait abstracts routing/signing
//! so the strategy code is identical in paper and live mode. Paper mode
//! simulates fills with configurable slippage + fees and NEVER touches real
//! funds. The Jupiter/Jito live implementation lives in `live.rs` (M3.5).

use crate::config::Config;
use crate::types::{Fill, Side};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Monetary resolution for fills and accounting, in decimal places. All fill
/// values are settled at this scale so that P&L conservation holds EXACTLY:
/// rust_decimal rounds at 28 *significant* digits, and unrounded products
/// (price × qty) carry ~28-digit fractions whose sums silently round — the
/// dust makes `equity == start + Σ realized` fail by ~1e-26. 12dp keeps every
/// accumulated sum ≤ ~20 significant digits, far inside the precision budget
/// (and far beyond real settlement resolution).
pub const FILL_DP: u32 = 12;

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("fill rejected: {0}")]
    Rejected(String),
    /// Transport / upstream failure (HTTP, RPC, bundle submission). Unlike a
    /// rejection the order state is UNKNOWN — callers must reconcile against
    /// on-chain state before re-issuing (spec §5.5), never blind-resubmit.
    #[error("upstream transport failure: {0}")]
    Transport(String),
    /// Live executor used before it is armed (paper mode, or live mode
    /// without signing wired up). Never moves funds by construction.
    #[error("live executor not armed: {0}")]
    NotArmed(String),
}

/// Jito/priority urgency tier for an order (M6). Exits are time-critical —
/// a stop that doesn't land is a bigger realized loss — while entries are
/// patient spray into fresh pools. The PAPER executor ignores tiers (its
/// fees are `fee_bps`-based); the live executor maps each tier to a
/// configured lamport tip + swap-request priority fee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipTier {
    /// Funnel-slice buy (patient spray).
    Entry,
    /// Flip-mode exit: stop/TP/time-stop — land it NOW.
    FlipExit,
    /// Conviction-trail exit — urgent, but the position already banked
    /// outlier gains, so it bids below a flip stop.
    ConvictionExit,
}

/// Order router abstraction. Async since M3.5: live impls (Jupiter quote →
/// swap tx → Jito bundle) await network I/O; the paper executor's bodies are
/// synchronous computes wrapped in async fns so the engine has one path.
///
/// Implementors must be `Send + Sync` (async_trait futures are `Send`).
#[async_trait]
pub trait Executor: Send + Sync {
    /// Buy `budget_usd` worth of `mint` at reference `price_usd`.
    ///
    /// `liquidity_usd` is the pool liquidity at decision time — the paper
    /// executor turns it into depth-aware slippage; live executors use it
    /// for preflight slippage-budget checks.
    ///
    /// `deadline` bounds the funnel window (spec §4: all slices land within
    /// `funnel_window_secs`). Live executors must refuse slices sent past it;
    /// the paper executor is instantaneous so the deadline is trivially met.
    #[allow(clippy::too_many_arguments)]
    async fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        deadline: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError>;

    /// Sell `qty` of `mint` at reference `price_usd`. `tier` selects the
    /// urgency (flip stops outbid conviction trails outbid entries) — the
    /// engine derives it from the position's hold mode.
    #[allow(clippy::too_many_arguments)]
    async fn sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
        tier: TipTier,
    ) -> Result<Fill, ExecError>;
}

/// Depth-aware slippage: `base + coeff * (notional / liquidity * 100)`,
/// capped at `max`. Pure function of Decimals — no floats, deterministic.
///
/// Intuition is constant-product price impact: a $1.25K slice into an $8K
/// bonding-curve pool moves the price ~15% before fees, while the same slice
/// into $1M of graduated liquidity costs ~base only. `liquidity <= 0` (unknown
/// depth) falls back to `base` rather than guessing.
pub fn effective_slippage_pct(
    base_pct: Decimal,
    impact_coeff: Decimal,
    max_pct: Decimal,
    notional_usd: Decimal,
    liquidity_usd: Decimal,
) -> Decimal {
    if liquidity_usd <= Decimal::ZERO || notional_usd <= Decimal::ZERO {
        return base_pct.min(max_pct);
    }
    let depth_pct = notional_usd / liquidity_usd * Decimal::from(100);
    (base_pct + impact_coeff * depth_pct).min(max_pct)
}

/// Paper executor: deterministic simulation, no RNG, no network.
///
/// - buys fill at `price * (1 + eff)`, sells at `price * (1 - eff)` where
///   `eff = base + coeff * (notional/liquidity)` capped at `max` (we cross
///   the spread against ourselves, and thin bonding-curve pools cost more);
/// - a `fee_bps` fee is charged on notional (Pump.fun-style ~1%);
/// - zero/negative budget or qty and non-positive prices are rejected.
pub struct PaperExecutor {
    slippage_pct: Decimal,
    impact_coeff: Decimal,
    max_slippage_pct: Decimal,
    fee_bps: Decimal,
    /// Basis points (0..=10_000) chance any given order fails, decided as a
    /// pure function of the order id (FNV-1a) — no RNG state, so replays stay
    /// bit-reproducible. 0 = never fail (default).
    failure_bps: u64,
}

impl PaperExecutor {
    pub fn new(cfg: &Config) -> PaperExecutor {
        PaperExecutor {
            slippage_pct: cfg.paper_slippage_pct,
            impact_coeff: cfg.paper_impact_coeff,
            max_slippage_pct: cfg.paper_max_slippage_pct,
            fee_bps: Decimal::from(cfg.fee_bps),
            failure_bps: 0,
        }
    }

    /// Chaos-mode constructor: a fraction of orders fail deterministically
    /// (keyed on order id), exercising the engine's partial-funnel and
    /// sell-retry/reconciliation paths.
    pub fn with_failure_bps(cfg: &Config, failure_bps: u64) -> PaperExecutor {
        PaperExecutor {
            slippage_pct: cfg.paper_slippage_pct,
            impact_coeff: cfg.paper_impact_coeff,
            max_slippage_pct: cfg.paper_max_slippage_pct,
            fee_bps: Decimal::from(cfg.fee_bps),
            failure_bps: failure_bps.min(10_000),
        }
    }

    /// Pure function of (order_id, rate): FNV-1a % 10_000 < rate.
    pub fn would_fail(order_id: &str, failure_bps: u64) -> bool {
        if failure_bps == 0 {
            return false;
        }
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in order_id.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h % 10_000 < failure_bps
    }

    fn fails(&self, order_id: &str) -> bool {
        Self::would_fail(order_id, self.failure_bps)
    }

    #[allow(clippy::too_many_arguments)]
    fn fill(
        &self,
        side: Side,
        mint: &str,
        qty: Decimal,
        ref_price: Decimal,
        eff_slip_pct: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        if ref_price <= Decimal::ZERO || qty <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "non-positive qty ({qty}) or price ({ref_price}) for {mint}"
            )));
        }
        let slip = Decimal::ONE + eff_slip_pct / Decimal::from(100);
        let anti = Decimal::ONE - eff_slip_pct / Decimal::from(100);
        let price = match side {
            Side::Buy => ref_price * slip,
            Side::Sell => ref_price * anti,
        };
        if price <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "slippage drove {side:?} price non-positive for {mint}"
            )));
        }
        let price = price.round_dp(FILL_DP);
        let qty = qty.round_dp(FILL_DP);
        let notional = (price * qty).round_dp(FILL_DP);
        let fee = (notional * self.fee_bps / dec!(10_000)).round_dp(FILL_DP);
        Ok(Fill {
            order_id: order_id.to_string(),
            mint: mint.to_string(),
            side,
            qty,
            price_usd: price,
            notional_usd: notional,
            fee_usd: fee,
            ts: now,
        })
    }
}

#[async_trait]
impl Executor for PaperExecutor {
    async fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        _deadline: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        if self.fails(order_id) {
            return Err(ExecError::Rejected(format!(
                "simulated fill failure (order {order_id})"
            )));
        }
        if budget_usd <= Decimal::ZERO || price_usd <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "non-positive budget ({budget_usd}) or price ({price_usd}) for {mint}"
            )));
        }
        let eff = effective_slippage_pct(
            self.slippage_pct,
            self.impact_coeff,
            self.max_slippage_pct,
            budget_usd,
            liquidity_usd,
        );
        let fill_price = price_usd * (Decimal::ONE + eff / Decimal::from(100));
        let qty = budget_usd / fill_price;
        self.fill(Side::Buy, mint, qty, price_usd, eff, now, order_id)
    }

    async fn sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
        _tier: TipTier,
    ) -> Result<Fill, ExecError> {
        // Paper fees are `fee_bps`-based — urgency tiers are a live-only
        // concept (Jito tip + priority fee), so the tier is ignored here.
        if self.fails(order_id) {
            return Err(ExecError::Rejected(format!(
                "simulated fill failure (order {order_id})"
            )));
        }
        let notional = qty * price_usd;
        let eff = effective_slippage_pct(
            self.slippage_pct,
            self.impact_coeff,
            self.max_slippage_pct,
            notional,
            liquidity_usd,
        );
        self.fill(Side::Sell, mint, qty, price_usd, eff, now, order_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use chrono::TimeZone;

    fn ts(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + s, 0).unwrap()
    }

    #[tokio::test]
    async fn buy_applies_slippage_and_fee() {
        let mut cfg = Config::paper_defaults();
        cfg.paper_impact_coeff = Decimal::ZERO; // isolate base slippage
        let mut ex = PaperExecutor::new(&cfg); // 2% slip, 100bps fee
        let f = ex
            .buy(
                "M",
                dec!(1000),
                dec!(1),
                dec!(1_000_000),
                ts(0),
                ts(5),
                "o1",
            )
            .await
            .unwrap();
        assert_eq!(f.side, Side::Buy);
        assert_eq!(f.price_usd, dec!(1.02));
        // qty = budget/fill_price, settled at FILL_DP.
        let expected_qty = (dec!(1000) / dec!(1.02)).round_dp(FILL_DP);
        assert_eq!(f.qty, expected_qty);
        // notional = price × qty (settled), fee = 1% of notional.
        let expected_notional = (f.price_usd * f.qty).round_dp(FILL_DP);
        assert_eq!(f.notional_usd, expected_notional);
        assert_eq!(
            f.fee_usd,
            (expected_notional * dec!(100) / dec!(10_000)).round_dp(FILL_DP)
        );
    }

    #[tokio::test]
    async fn sell_applies_anti_slippage() {
        let mut cfg = Config::paper_defaults();
        cfg.paper_impact_coeff = Decimal::ZERO;
        let mut ex = PaperExecutor::new(&cfg);
        let f = ex
            .sell(
                "M",
                dec!(1000),
                dec!(2),
                dec!(1_000_000),
                ts(0),
                "o2",
                TipTier::FlipExit,
            )
            .await
            .unwrap();
        assert_eq!(f.price_usd, dec!(1.96)); // 2 * 0.98
        assert_eq!(f.notional_usd, dec!(1960));
        assert_eq!(f.fee_usd, dec!(19.60));
    }

    #[test]
    fn depth_impact_scales_with_notional_over_liquidity() {
        // base 2% + 1.0 * (1000/8000*100=12.5%) = 14.5% on thin curve pool.
        assert_eq!(
            effective_slippage_pct(dec!(2), dec!(1), dec!(50), dec!(1000), dec!(8000)),
            dec!(14.5)
        );
        // Same slice into deep graduated liquidity costs ~base only.
        assert_eq!(
            effective_slippage_pct(dec!(2), dec!(1), dec!(50), dec!(1000), dec!(1_000_000)),
            dec!(2.1)
        );
        // Cap binds on whale-into-puddle trades.
        assert_eq!(
            effective_slippage_pct(dec!(2), dec!(1), dec!(50), dec!(8000), dec!(8000)),
            dec!(50)
        );
        // Unknown depth falls back to base (never guesses).
        assert_eq!(
            effective_slippage_pct(dec!(2), dec!(1), dec!(50), dec!(1000), Decimal::ZERO),
            dec!(2)
        );
    }

    #[tokio::test]
    async fn thin_pool_buy_pays_impact_and_thick_pool_does_not() {
        let mut ex = PaperExecutor::new(&Config::paper_defaults()); // base 2, coeff 1
                                                                    // $1000 into $8000 pool → 14.5% slippage → fill at 1.145.
        let thin = ex
            .buy("M", dec!(1000), dec!(1), dec!(8000), ts(0), ts(5), "thin")
            .await
            .unwrap();
        assert_eq!(thin.price_usd, dec!(1.145));
        // Same $1000 into $1M pool → 2.1% → fill at 1.021.
        let thick = ex
            .buy(
                "M",
                dec!(1000),
                dec!(1),
                dec!(1_000_000),
                ts(0),
                ts(5),
                "thick",
            )
            .await
            .unwrap();
        assert_eq!(thick.price_usd, dec!(1.021));
        assert!(thin.price_usd > thick.price_usd);
    }

    #[tokio::test]
    async fn rejects_garbage() {
        let mut ex = PaperExecutor::new(&Config::paper_defaults());
        let liq = dec!(1_000_000);
        assert!(ex
            .buy("M", Decimal::ZERO, dec!(1), liq, ts(0), ts(5), "o")
            .await
            .is_err());
        assert!(ex
            .sell("M", dec!(0), dec!(1), liq, ts(0), "o", TipTier::FlipExit)
            .await
            .is_err());
        assert!(ex
            .buy("M", dec!(10), Decimal::ZERO, liq, ts(0), ts(5), "o")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn deterministic_failure_injection() {
        let cfg = Config::paper_defaults();
        let liq = dec!(1_000_000);
        // Default rate 0: never fails.
        let mut ex = PaperExecutor::new(&cfg);
        assert!(ex
            .buy("M", dec!(10), dec!(1), liq, ts(0), ts(5), "id1")
            .await
            .is_ok());

        // Rate 100%: always fails, and the same order id always fails.
        let mut ex = PaperExecutor::with_failure_bps(&cfg, 10_000);
        assert!(ex
            .buy("M", dec!(10), dec!(1), liq, ts(0), ts(5), "x")
            .await
            .is_err());
        assert!(ex
            .buy("M", dec!(10), dec!(1), liq, ts(0), ts(5), "x")
            .await
            .is_err());
        assert!(ex
            .sell(
                "M",
                dec!(10),
                dec!(1),
                liq,
                ts(0),
                "x",
                TipTier::ConvictionExit
            )
            .await
            .is_err());

        // The decision is a pure function of (order_id, rate).
        assert_eq!(
            PaperExecutor::would_fail("order-abc", 5_000),
            PaperExecutor::would_fail("order-abc", 5_000)
        );
        // Rate 0 never fails for any id.
        assert!(!PaperExecutor::would_fail("order-abc", 0));
    }
}
