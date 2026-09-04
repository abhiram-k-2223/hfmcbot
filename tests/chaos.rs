//! Chaos + property tests (spec §7): deterministic random event streams through
//! the full pipeline, asserting state invariants regardless of which simulated
//! orders fail. No external RNG dependency — xorshift64 with fixed seeds keeps
//! runs bit-reproducible.

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use hfmcbot::config::Config;
use hfmcbot::engine::Engine;
use hfmcbot::exec::{ExecError, Executor, PaperExecutor};
use hfmcbot::persist::AuditLog;
use hfmcbot::types::{Event, Fill, Launch, Launchpad, PriceUpdate};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Deterministic xorshift64 — fixed seed → identical stream every run.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> XorShift {
        XorShift(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
}

fn launch(mint: &str, at: i64, price: Decimal) -> Event {
    Event::Launch(Launch {
        mint: mint.into(),
        launchpad: Launchpad::PumpFun,
        created_at: ts(at),
        creator_hold_pct: dec!(2),
        mint_renounced: false, // on-curve
        is_honeypot: false,
        liquidity_usd: dec!(9000),
        on_curve: true,
        price_usd: price,
    })
}

fn price(mint: &str, at: i64, p: Decimal) -> Event {
    Event::Price(PriceUpdate {
        mint: mint.into(),
        ts: ts(at),
        price_usd: p,
        liquidity_usd: dec!(9000),
    })
}

/// Random launch + multiplicative random-walk price stream, ascending in time.
fn random_stream(rng: &mut XorShift) -> Vec<Event> {
    let mut events = Vec::new();
    let tokens = 4 + rng.below(5) as usize;
    for i in 0..tokens {
        let mint = format!("T{i}");
        let at = (i as u64 * rng.below(120)) as i64;
        events.push(launch(&mint, at, dec!(0.001)));
        let mut p = dec!(0.001);
        let ticks = 5 + rng.below(12);
        let mut t = at + 1 + rng.below(600) as i64;
        for _ in 0..ticks {
            // ×0.50 .. ×1.69 per tick, clamped positive.
            let f = 50 + rng.below(120);
            p *= Decimal::from(f);
            p /= Decimal::from(100);
            if p < dec!(0.0000001) {
                p = dec!(0.0000001);
            }
            t += 1 + rng.below(80_000) as i64;
            events.push(price(&mint, t, p));
        }
    }
    events.sort_by_key(|e| e.ts());
    events
}

/// PROPERTY: funnel slices always sum exactly to the total, never negative,
/// and are either all positive or collapsed to a single slice.
#[test]
fn prop_funnel_slices_always_sum_to_total() {
    let mut rng = XorShift::new(0xC0FFEE);
    for _ in 0..500 {
        let n = 1 + rng.below(6) as usize;
        let total = Decimal::from(rng.below(100_000_000)) / Decimal::from(10_000);
        let slices = hfmcbot::strategy::split_into_slices(total, n);
        assert_eq!(slices.iter().copied().sum::<Decimal>(), total);
        assert!(slices.iter().all(|s| *s >= Decimal::ZERO));
        assert!(slices.len() == 1 || slices.iter().all(|s| *s > Decimal::ZERO));
    }
}

/// PROPERTY: over random event streams with a fraction of orders failing,
/// P&L conservation and deployed-capital reconciliation always hold.
#[tokio::test]
async fn prop_random_streams_preserve_invariants() {
    let cfg = Config::paper_defaults();
    for seed in 1..=8u64 {
        let mut rng = XorShift::new(seed * 7_919);
        let events = random_stream(&mut rng);
        let dir = tempfile::tempdir().unwrap();
        let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
        let failure_bps = (seed * 1_237) % 6_001; // 0..=40% chaos
        let executor = PaperExecutor::with_failure_bps(&cfg, failure_bps);
        let mut eng = Engine::new(cfg.clone(), Box::new(executor), audit);
        for ev in &events {
            eng.on_event(ev).await;
        }
        eng.check_invariants()
            .unwrap_or_else(|e| panic!("seed {seed} (failure {failure_bps}bps): {e}"));
    }
}

/// All orders failing → no position ever opens, state stays exactly at start.
#[tokio::test]
async fn all_orders_failing_leaves_state_untouched() {
    let cfg = Config::paper_defaults();
    let dir = tempfile::tempdir().unwrap();
    let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
    let executor = PaperExecutor::with_failure_bps(&cfg, 10_000);
    let mut eng = Engine::new(cfg.clone(), Box::new(executor), audit);

    eng.on_event(&launch("GHOST", 0, dec!(0.001))).await;
    eng.on_event(&launch("GHOST2", 65, dec!(0.001))).await;

    assert_eq!(eng.open_positions(), 0);
    assert_eq!(eng.deployed_usd(), Decimal::ZERO);
    assert_eq!(eng.equity(), cfg.equity_usd);
    eng.check_invariants().unwrap();
}

/// Wrapper that fails the FIRST sell attempt per mint, then succeeds —
/// deterministic chaos for the exit/reconciliation path.
struct FailFirstSell {
    inner: PaperExecutor,
    sell_attempts: HashMap<String, u64>,
}

#[async_trait]
impl Executor for FailFirstSell {
    async fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        deadline: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
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
        mint: &str,
        qty: Decimal,
        price_usd: Decimal,
        liquidity_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
        tier: hfmcbot::exec::TipTier,
    ) -> Result<Fill, ExecError> {
        let n = self.sell_attempts.entry(mint.to_string()).or_insert(0);
        *n += 1;
        if *n == 1 {
            return Err(ExecError::Rejected("chaos: first sell fails".into()));
        }
        self.inner
            .sell(mint, qty, price_usd, liquidity_usd, now, order_id, tier)
            .await
    }
}

/// A failed sell must NOT close the position or corrupt state; the exit is
/// retried on the next tick with a fresh order id and then lands.
#[tokio::test]
async fn failed_sell_is_retried_and_state_stays_consistent() {
    let cfg = Config::paper_defaults();
    let dir = tempfile::tempdir().unwrap();
    let audit = AuditLog::open(&dir.path().join("audit.jsonl")).unwrap();
    let executor = FailFirstSell {
        inner: PaperExecutor::new(&cfg),
        sell_attempts: HashMap::new(),
    };
    let mut eng = Engine::new(cfg.clone(), Box::new(executor), audit);

    eng.on_event(&launch("RETRY", 0, dec!(0.001))).await;
    assert_eq!(eng.open_positions(), 1);

    // -40% tick → stop_loss exit decision; the first sell attempt fails.
    eng.on_event(&price("RETRY", 10, dec!(0.0006))).await;
    assert_eq!(
        eng.open_positions(),
        1,
        "failed sell must not close the position"
    );
    eng.check_invariants().unwrap();

    // Next tick retries the exit and lands it.
    eng.on_event(&price("RETRY", 11, dec!(0.0006))).await;
    assert_eq!(eng.open_positions(), 0);
    assert_eq!(eng.closed_trades().len(), 1);
    assert_eq!(eng.closed_trades()[0].exit_reason, "stop_loss");
    eng.check_invariants().unwrap();
}
