//! Execution module (spec §5). The `Executor` trait abstracts routing/signing
//! so the strategy code is identical in paper and live mode. Paper mode
//! simulates fills with configurable slippage + fees and NEVER touches real
//! funds. The Jito/Jupiter live implementation lands in M3/M4.

use crate::config::Config;
use crate::types::{Fill, Side};
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
}

/// Order router abstraction. Live impls (Jupiter quote → swap tx → Jito bundle)
/// will implement the same interface.
pub trait Executor {
    /// Buy `budget_usd` worth of `mint` at reference `price_usd`.
    ///
    /// `deadline` bounds the funnel window (spec §4: all slices land within
    /// `funnel_window_secs`). Live executors must refuse slices sent past it;
    /// the paper executor is instantaneous so the deadline is trivially met.
    fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        price_usd: Decimal,
        now: DateTime<Utc>,
        deadline: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError>;

    /// Sell `qty` of `mint` at reference `price_usd`.
    fn sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        price_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError>;
}

/// Paper executor: deterministic simulation, no RNG, no network.
///
/// - buys fill at `price * (1 + slippage)`, sells at `price * (1 - slippage)`
///   (we cross the spread against ourselves);
/// - a `fee_bps` fee is charged on notional (Pump.fun-style ~1%);
/// - zero/negative budget or qty and non-positive prices are rejected.
pub struct PaperExecutor {
    slippage_pct: Decimal,
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

    fn fill(
        &self,
        side: Side,
        mint: &str,
        qty: Decimal,
        ref_price: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        if ref_price <= Decimal::ZERO || qty <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "non-positive qty ({qty}) or price ({ref_price}) for {mint}"
            )));
        }
        let slip = Decimal::ONE + self.slippage_pct / Decimal::from(100);
        let anti = Decimal::ONE - self.slippage_pct / Decimal::from(100);
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

impl Executor for PaperExecutor {
    fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        price_usd: Decimal,
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
        let fill_price = price_usd * (Decimal::ONE + self.slippage_pct / Decimal::from(100));
        let qty = budget_usd / fill_price;
        self.fill(Side::Buy, mint, qty, price_usd, now, order_id)
    }

    fn sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        price_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        if self.fails(order_id) {
            return Err(ExecError::Rejected(format!(
                "simulated fill failure (order {order_id})"
            )));
        }
        self.fill(Side::Sell, mint, qty, price_usd, now, order_id)
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

    #[test]
    fn buy_applies_slippage_and_fee() {
        let mut ex = PaperExecutor::new(&Config::paper_defaults()); // 2% slip, 100bps fee
        let f = ex.buy("M", dec!(1000), dec!(1), ts(0), ts(5), "o1").unwrap();
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

    #[test]
    fn sell_applies_anti_slippage() {
        let mut ex = PaperExecutor::new(&Config::paper_defaults());
        let f = ex
            .sell("M", dec!(1000), dec!(2), ts(0), "o2")
            .unwrap();
        assert_eq!(f.price_usd, dec!(1.96)); // 2 * 0.98
        assert_eq!(f.notional_usd, dec!(1960));
        assert_eq!(f.fee_usd, dec!(19.60));
    }

    #[test]
    fn rejects_garbage() {
        let mut ex = PaperExecutor::new(&Config::paper_defaults());
        assert!(ex.buy("M", Decimal::ZERO, dec!(1), ts(0), ts(5), "o").is_err());
        assert!(ex.sell("M", dec!(0), dec!(1), ts(0), "o").is_err());
        assert!(ex.buy("M", dec!(10), Decimal::ZERO, ts(0), ts(5), "o").is_err());
    }

    #[test]
    fn deterministic_failure_injection() {
        let cfg = Config::paper_defaults();
        // Default rate 0: never fails.
        let mut ex = PaperExecutor::new(&cfg);
        assert!(ex.buy("M", dec!(10), dec!(1), ts(0), ts(5), "id1").is_ok());

        // Rate 100%: always fails, and the same order id always fails.
        let mut ex = PaperExecutor::with_failure_bps(&cfg, 10_000);
        assert!(ex.buy("M", dec!(10), dec!(1), ts(0), ts(5), "x").is_err());
        assert!(ex.buy("M", dec!(10), dec!(1), ts(0), ts(5), "x").is_err());
        assert!(ex.sell("M", dec!(10), dec!(1), ts(0), "x").is_err());

        // The decision is a pure function of (order_id, rate).
        assert_eq!(
            PaperExecutor::would_fail("order-abc", 5_000),
            PaperExecutor::would_fail("order-abc", 5_000)
        );
        // Rate 0 never fails for any id.
        assert!(!PaperExecutor::would_fail("order-abc", 0));
    }
}
