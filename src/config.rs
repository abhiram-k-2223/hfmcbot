//! Config — every trading threshold is env-driven (spec §4/§9: no hardcoded magic numbers).
//!
//! All keys are prefixed `HFM_`. See `.env.example` for the full list.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::env;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Paper,
    Live,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mode::Paper => write!(f, "paper"),
            Mode::Live => write!(f, "live"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub mode: Mode,

    // capital
    pub equity_usd: Decimal,

    // entry gate
    pub entry_max_age_secs: u64,
    pub min_liquidity_usd: Decimal,
    pub max_dev_hold_pct: Decimal,
    pub max_slippage_pct: Decimal,

    // funnel entry
    pub funnel_slices: usize,
    pub funnel_window_secs: u64,

    // sizing
    pub risk_per_trade_pct: Decimal,
    pub max_single_pos_usd: Decimal,
    pub max_open_positions: usize,

    // exit: flip mode
    pub take_profit_pct: Decimal,
    pub stop_loss_pct: Decimal,
    pub max_hold_secs: u64,

    // exit: conviction mode
    pub conviction_min_pct: Decimal,
    pub trail_pct: Decimal,
    pub conviction_max_hold_secs: u64,

    // risk & controls
    pub daily_loss_limit_pct: Decimal,
    pub max_trades_per_min: u32,
    pub kill_switch: bool,

    // paper execution
    pub paper_slippage_pct: Decimal,
    pub fee_bps: u64,

    // persistence
    pub audit_log_path: String,
    pub replay_events_path: Option<String>,

    // upstreams (M1+)
    pub rpc_url: String,
    pub ws_url: String,
    pub jito_url: String,
    pub jito_tip_lamports: u64,
}

impl Config {
    /// Load from process env (after `dotenvy` has loaded `.env`).
    pub fn from_env() -> Result<Config, String> {
        let mode = match env_str("HFM_MODE", "paper").to_lowercase().as_str() {
            "paper" => Mode::Paper,
            "live" => Mode::Live,
            other => return Err(format!("invalid HFM_MODE '{other}' (expected paper|live)")),
        };

        let cfg = Config {
            mode,
            equity_usd: env_dec("HFM_EQUITY_USD", dec!(50000))?,
            entry_max_age_secs: env_u64("HFM_ENTRY_MAX_AGE_SECS", 600)?,
            min_liquidity_usd: env_dec("HFM_MIN_LIQUIDITY_USD", dec!(50000))?,
            max_dev_hold_pct: env_dec("HFM_MAX_DEV_HOLD_PCT", dec!(10))?,
            max_slippage_pct: env_dec("HFM_MAX_SLIPPAGE_PCT", dec!(10))?,
            funnel_slices: env_u64("HFM_FUNNEL_SLICES", 3)? as usize,
            funnel_window_secs: env_u64("HFM_FUNNEL_WINDOW_SECS", 5)?,
            risk_per_trade_pct: env_dec("HFM_RISK_PER_TRADE_PCT", dec!(2.5))?,
            max_single_pos_usd: env_dec("HFM_MAX_SINGLE_POS_USD", dec!(5000))?,
            max_open_positions: env_u64("HFM_MAX_OPEN_POSITIONS", 12)? as usize,
            take_profit_pct: env_dec("HFM_TAKE_PROFIT_PCT", dec!(120))?,
            stop_loss_pct: env_dec("HFM_STOP_LOSS_PCT", dec!(30))?,
            max_hold_secs: env_u64("HFM_MAX_HOLD_SECS", 21_600)?,
            conviction_min_pct: env_dec("HFM_CONVICTION_MIN_PCT", dec!(300))?,
            trail_pct: env_dec("HFM_TRAIL_PCT", dec!(25))?,
            conviction_max_hold_secs: env_u64("HFM_CONVICTION_MAX_HOLD_DAYS", 15)? * 86_400,
            daily_loss_limit_pct: env_dec("HFM_DAILY_LOSS_LIMIT_PCT", dec!(10))?,
            max_trades_per_min: env_u64("HFM_MAX_TRADES_PER_MIN", 6)? as u32,
            kill_switch: env_bool("HFM_KILL_SWITCH", false)?,
            paper_slippage_pct: env_dec("HFM_PAPER_SLIPPAGE_PCT", dec!(2))?,
            fee_bps: env_u64("HFM_FEE_BPS", 100)?,
            audit_log_path: env_str("HFM_AUDIT_LOG_PATH", "data/audit.jsonl"),
            replay_events_path: env_non_empty("HFM_REPLAY_EVENTS_PATH"),
            rpc_url: env_str("HFM_RPC_URL", "https://api.mainnet-beta.solana.com"),
            ws_url: env_str("HFM_WS_URL", "wss://api.mainnet-beta.solana.com"),
            jito_url: env_str("HFM_JITO_URL", "https://mainnet.block-engine.jito.wtf"),
            jito_tip_lamports: env_u64("HFM_JITO_TIP_LAMPORTS", 1_000_000)?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Fail fast on nonsensical configuration rather than trading blind.
    pub fn validate(&self) -> Result<(), String> {
        let pos = |v: Decimal, name: &str| -> Result<(), String> {
            if v <= Decimal::ZERO {
                return Err(format!("{name} must be > 0, got {v}"));
            }
            Ok(())
        };
        pos(self.equity_usd, "HFM_EQUITY_USD")?;
        pos(self.min_liquidity_usd, "HFM_MIN_LIQUIDITY_USD")?;
        pos(self.max_single_pos_usd, "HFM_MAX_SINGLE_POS_USD")?;
        pos(self.take_profit_pct, "HFM_TAKE_PROFIT_PCT")?;
        pos(self.stop_loss_pct, "HFM_STOP_LOSS_PCT")?;
        pos(self.trail_pct, "HFM_TRAIL_PCT")?;
        pos(self.risk_per_trade_pct, "HFM_RISK_PER_TRADE_PCT")?;
        if self.funnel_slices == 0 {
            return Err("HFM_FUNNEL_SLICES must be >= 1".into());
        }
        if self.max_open_positions == 0 {
            return Err("HFM_MAX_OPEN_POSITIONS must be >= 1".into());
        }
        if self.max_trades_per_min == 0 {
            return Err("HFM_MAX_TRADES_PER_MIN must be >= 1".into());
        }
        // NOTE: HFM_TAKE_PROFIT_PCT < HFM_CONVICTION_MIN_PCT is valid and is the
        // spec default: climbers that pass +TP exit; only gap-movers that are
        // already past +conviction_min at a tick get promoted to trail mode.
        if self.mode == Mode::Live {
            return Err(
                "HFM_MODE=live is not enabled yet (M4+); run paper mode first — see README".into(),
            );
        }
        Ok(())
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_non_empty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn env_dec(key: &str, default: Decimal) -> Result<Decimal, String> {
    match env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse::<Decimal>()
            .map_err(|e| format!("{key}: invalid decimal '{raw}': {e}")),
        Err(_) => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("{key}: invalid integer '{raw}': {e}")),
        Err(_) => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match env::var(key) {
        Ok(raw) => match raw.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(format!("{key}: invalid bool '{other}'")),
        },
        Err(_) => Ok(default),
    }
}


/// Canonical paper-mode defaults — mirrors `.env.example` exactly. Public so
/// tests, tools, and integration replays can get a valid Config without env vars.
impl Config {
    pub fn paper_defaults() -> Config {
        Config {
            mode: Mode::Paper,
            equity_usd: dec!(50000),
            entry_max_age_secs: 600,
            min_liquidity_usd: dec!(50000),
            max_dev_hold_pct: dec!(10),
            max_slippage_pct: dec!(10),
            funnel_slices: 3,
            funnel_window_secs: 5,
            risk_per_trade_pct: dec!(2.5),
            max_single_pos_usd: dec!(5000),
            max_open_positions: 12,
            take_profit_pct: dec!(120),
            stop_loss_pct: dec!(30),
            max_hold_secs: 21_600,
            conviction_min_pct: dec!(300),
            trail_pct: dec!(25),
            conviction_max_hold_secs: 15 * 86_400,
            daily_loss_limit_pct: dec!(10),
            max_trades_per_min: 6,
            kill_switch: false,
            paper_slippage_pct: dec!(2),
            fee_bps: 100,
            audit_log_path: "/tmp/hfmcbot-test-audit.jsonl".into(),
            replay_events_path: None,
            rpc_url: "https://example.invalid".into(),
            ws_url: "wss://example.invalid".into(),
            jito_url: "https://example.invalid".into(),
            jito_tip_lamports: 1_000_000,
        }
    }
}

