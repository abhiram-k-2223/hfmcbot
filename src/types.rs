//! Domain types. NO floating point for money — `Decimal` everywhere (spec §9).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Launchpads the bot targets (REPORT.md part 4: pump.fun, stonkfun, meteora_virtual_curve).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Launchpad {
    #[serde(rename = "pumpfun")]
    PumpFun,
    Stonkfun,
    MeteoraVirtualCurve,
}

impl Launchpad {
    pub fn as_str(&self) -> &'static str {
        match self {
            Launchpad::PumpFun => "pumpfun",
            Launchpad::Stonkfun => "stonkfun",
            Launchpad::MeteoraVirtualCurve => "meteora_virtual_curve",
        }
    }
}

/// A fresh-launch event from the ingest feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Launch {
    pub mint: String,
    pub launchpad: Launchpad,
    pub created_at: DateTime<Utc>,
    /// % of supply held by creator (security guard).
    pub creator_hold_pct: Decimal,
    /// Mint authority renounced (required once the token is on a migrated/graduated pool).
    pub mint_renounced: bool,
    /// Honeypot flag from security checks.
    pub is_honeypot: bool,
    /// Current pool liquidity in USD.
    pub liquidity_usd: Decimal,
    /// True while the token is still on its bonding curve (pump.fun/stonkfun
    /// virtual curve); false once migrated/graduated to a real AMM pool.
    pub on_curve: bool,
    /// Price at detection, USD per token unit.
    pub price_usd: Decimal,
}

/// Continuous price/book update for a tracked token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    pub mint: String,
    pub ts: DateTime<Utc>,
    pub price_usd: Decimal,
    pub liquidity_usd: Decimal,
}

/// Ingest events fed into the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Launch(Launch),
    Price(PriceUpdate),
}

impl Event {
    pub fn ts(&self) -> DateTime<Utc> {
        match self {
            Event::Launch(l) => l.created_at,
            Event::Price(p) => p.ts,
        }
    }
}

/// Exit regime for an open position (REPORT.md: two hold modes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HoldMode {
    /// Fast flip: TP/SL/time-stop.
    Flip,
    /// Conviction: trail the breakout, allow multi-day holds.
    Conviction,
}

/// An open position (accumulated across funnel slices).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: String,
    pub launchpad: Launchpad,
    pub opened_at: DateTime<Utc>,
    /// Volume-weighted average entry price, USD per token unit.
    pub entry_price: Decimal,
    /// Total token units held.
    pub qty: Decimal,
    /// Total USD spent (incl. fees).
    pub cost_usd: Decimal,
    /// Highest price seen since entry (stop / trail reference).
    pub high_water: Decimal,
    /// Last observed price (fill or tick) — the reference for time-stop
    /// sweeps when no fresh tick is available.
    pub last_price: Decimal,
    /// Last observed pool liquidity — the depth reference for time-stop
    /// sweeps (which have no fresh `PriceUpdate` to draw liquidity from).
    pub last_liquidity_usd: Decimal,
    pub mode: HoldMode,
    /// Consecutive failed exit attempts (M3.5 reconciliation, spec §5.5).
    #[serde(default)]
    pub exit_attempts: u32,
    /// True once `exit_attempts` reaches `max_exit_attempts`: the token is
    /// treated as unsellable (dead pool / no route). The position stays fully
    /// accounted in `deployed_usd` — the funds really are stuck — but it is
    /// surfaced in metrics/summary and skipped by the time-stop sweep (never
    /// blind-resubmit); fresh price ticks still retry the exit.
    #[serde(default)]
    pub stuck: bool,
}

impl Position {
    pub fn unrealized_pnl_usd(&self, price: Decimal) -> Decimal {
        price * self.qty - self.cost_usd
    }

    pub fn unrealized_pnl_pct(&self, price: Decimal) -> Decimal {
        if self.entry_price.is_zero() {
            return Decimal::ZERO;
        }
        ((price - self.entry_price) / self.entry_price) * Decimal::from(100)
    }
}

/// Side of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// A simulated or real fill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: String,
    pub mint: String,
    pub side: Side,
    pub qty: Decimal,
    pub price_usd: Decimal,
    pub notional_usd: Decimal,
    pub fee_usd: Decimal,
    pub ts: DateTime<Utc>,
}

/// Realized result of closing a position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub mint: String,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub qty: Decimal,
    pub cost_usd: Decimal,
    pub proceeds_usd: Decimal,
    pub pnl_usd: Decimal,
    pub pnl_pct: Decimal,
    pub exit_reason: String,
    pub mode: HoldMode,
}
