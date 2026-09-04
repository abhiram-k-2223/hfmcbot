//! Config — every trading threshold is env-driven (spec §4/§9: no hardcoded magic numbers).
//!
//! All keys are prefixed `HFM_`. See `.env.example` for the full list.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::env;
use std::fmt;
use std::str::FromStr;

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
    /// Failed sell attempts before a position is marked stuck/unsellable
    /// (M3.5 reconciliation — spec §5.5: reconcile, don't blind-resubmit).
    pub max_exit_attempts: u32,

    // exit: conviction mode
    pub conviction_min_pct: Decimal,
    pub trail_pct: Decimal,
    pub conviction_max_hold_secs: u64,

    // risk & controls
    pub daily_loss_limit_pct: Decimal,
    pub max_trades_per_min: u32,
    pub kill_switch: bool,

    // paper execution (depth-aware slippage: base + coeff * notional/liq, capped)
    pub paper_slippage_pct: Decimal,
    pub paper_impact_coeff: Decimal,
    pub paper_max_slippage_pct: Decimal,
    pub fee_bps: u64,

    // observability (M0)
    pub metrics_addr: String,
    pub heartbeat_secs: u64,

    // persistence
    pub audit_log_path: String,
    pub replay_events_path: Option<String>,
    /// Postgres audit mirror DSN (`--features pg` only). Blank = off (the
    /// JSONL trail is always on). Example:
    /// `postgres://bot@localhost/hfmcbot` — never commit credentials.
    pub pg_dsn: String,

    // upstreams (M1+)
    pub rpc_url: String,
    /// Ordered RPC fallbacks (M6 failover): tried after the primary, in
    /// order, on transport errors only. Empty = primary only.
    pub rpc_fallback_urls: Vec<String>,
    pub ws_url: String,
    pub jito_url: String,
    pub jito_tip_lamports: u64,
    /// M6 urgency tiers (each defaults to `jito_tip_lamports`, so one knob
    /// still prices everything): flip stops outbid conviction trails
    /// outbid entries.
    pub jito_tip_entry_lamports: u64,
    pub jito_tip_flip_lamports: u64,
    pub jito_tip_conviction_lamports: u64,
    /// Jito tip destination accounts (tip-transfer ix): CSV of base58
    /// pubkeys, round-robined per bundle. Empty = unset — real sends are
    /// refused until at least one is configured (fail-closed: a bundle
    /// without the tip transfer defeats the urgency tiers). Operators should
    /// list Jito's published tip accounts here. Each entry is validated as a
    /// 32-byte base58 pubkey at boot; tip accounts are public, safe to log.
    pub jito_tip_accounts: Vec<String>,

    // live execution (M3.5): Jupiter Swap API v1. The v1 API requires an
    // `x-api-key` header (the old keyless v6 endpoint is sunset) — empty key
    // means unauthenticated, which the API rate-limits hard.
    pub jupiter_url: String,
    pub jupiter_api_key: String,
    /// Sustained Jupiter quote rate; doubles as the token-bucket burst.
    pub jupiter_qps: u32,
    /// M4 live-execution safety + plumbing:
    /// - `simulate_only` (default true): the armed executor assembles and
    ///   SIGNS real swap transactions but never submits them — fills are
    ///   reported from quote math and loudly logged as simulated. Sending
    ///   requires explicitly opting out.
    /// - `sol_usd`: live SOL/USD reference for USD→lamport conversion on the
    ///   buy path (SOL is the quote currency). 0 = unset.
    /// - `bundle_timeout_secs`: how long landing reconciliation polls Jito
    ///   before declaring order state unknown (spec §5.5).
    pub simulate_only: bool,
    pub sol_usd: Decimal,
    pub bundle_timeout_secs: u64,
    /// Blockhash manager: seconds a fetched blockhash is reused across
    /// bundles before refetching. Bounds staleness without a per-bundle RPC
    /// round trip (distinct txs may share a recent blockhash safely).
    pub blockhash_ttl_secs: u64,
    /// Post-landing reconciliation dust: token-balance mismatch within this
    /// many tokens of the claimed fill passes; beyond it the fill is UNKNOWN
    /// (Transport). Covers route dust without blessing real divergence.
    pub reconcile_dust_tokens: Decimal,

    // live feed (M1): Yellowstone gRPC endpoint. Empty = unconfigured; the
    // feed refuses to connect until both are set (soak step needs credentials).
    pub geyser_url: String,
    pub geyser_token: String,
    /// Stream commitment: processed | confirmed | finalized.
    pub feed_commitment: String,
    /// Max concurrent per-token trade subscriptions on the WS price-path feed
    /// (M5): trades are subscribed ONLY for held/shortlisted tokens (D13
    /// fair-use). Must cover the open-position cap with headroom; the sub
    /// planner refuses to exceed it and drops excess new subs (never evicts
    /// held tokens to make room).
    pub max_trade_subs: usize,
}

impl Config {
    /// Load from process env (after `dotenvy` has loaded `.env`).
    pub fn from_env() -> Result<Config, String> {
        let mode = match env_str("HFM_MODE", "paper").to_lowercase().as_str() {
            "paper" => Mode::Paper,
            "live" => Mode::Live,
            other => return Err(format!("invalid HFM_MODE '{other}' (expected paper|live)")),
        };

        // Base tip first: each urgency tier defaults to it unless
        // explicitly priced.
        let jito_tip_lamports = env_u64("HFM_JITO_TIP_LAMPORTS", 1_000_000)?;

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
            max_exit_attempts: env_u64("HFM_MAX_EXIT_ATTEMPTS", 5)? as u32,
            conviction_min_pct: env_dec("HFM_CONVICTION_MIN_PCT", dec!(300))?,
            trail_pct: env_dec("HFM_TRAIL_PCT", dec!(25))?,
            conviction_max_hold_secs: env_u64("HFM_CONVICTION_MAX_HOLD_DAYS", 15)? * 86_400,
            daily_loss_limit_pct: env_dec("HFM_DAILY_LOSS_LIMIT_PCT", dec!(10))?,
            max_trades_per_min: env_u64("HFM_MAX_TRADES_PER_MIN", 6)? as u32,
            kill_switch: env_bool("HFM_KILL_SWITCH", false)?,
            paper_slippage_pct: env_dec("HFM_PAPER_SLIPPAGE_PCT", dec!(2))?,
            paper_impact_coeff: env_dec("HFM_PAPER_IMPACT_COEFF", dec!(1))?,
            paper_max_slippage_pct: env_dec("HFM_PAPER_MAX_SLIPPAGE_PCT", dec!(50))?,
            fee_bps: env_u64("HFM_FEE_BPS", 100)?,
            metrics_addr: env_str("HFM_METRICS_ADDR", "127.0.0.1:9898"),
            heartbeat_secs: env_u64("HFM_HEARTBEAT_SECS", 60)?,
            audit_log_path: env_str("HFM_AUDIT_LOG_PATH", "data/audit.jsonl"),
            replay_events_path: env_non_empty("HFM_REPLAY_EVENTS_PATH"),
            pg_dsn: env_str("HFM_PG_DSN", ""),
            rpc_url: env_str("HFM_RPC_URL", "https://api.mainnet-beta.solana.com"),
            rpc_fallback_urls: env_csv("HFM_RPC_FALLBACK_URLS"),
            ws_url: env_str("HFM_WS_URL", "wss://api.mainnet-beta.solana.com"),
            jito_url: env_str("HFM_JITO_URL", "https://mainnet.block-engine.jito.wtf"),
            jito_tip_lamports,
            jito_tip_entry_lamports: env_u64("HFM_JITO_TIP_ENTRY_LAMPORTS", jito_tip_lamports)?,
            jito_tip_flip_lamports: env_u64("HFM_JITO_TIP_FLIP_LAMPORTS", jito_tip_lamports)?,
            jito_tip_conviction_lamports: env_u64(
                "HFM_JITO_TIP_CONVICTION_LAMPORTS",
                jito_tip_lamports,
            )?,
            jito_tip_accounts: env_csv("HFM_JITO_TIP_ACCOUNT"),
            jupiter_url: env_str("HFM_JUPITER_URL", "https://api.jup.ag/swap/v1"),
            jupiter_api_key: env_str("HFM_JUPITER_API_KEY", ""),
            jupiter_qps: env_u64("HFM_JUPITER_QPS", 1)? as u32,
            simulate_only: env_bool("HFM_SIMULATE_ONLY", true)?,
            sol_usd: env_dec("HFM_SOL_USD", dec!(0))?,
            bundle_timeout_secs: env_u64("HFM_BUNDLE_TIMEOUT_SECS", 60)?,
            blockhash_ttl_secs: env_u64("HFM_BLOCKHASH_TTL_SECS", 30)?,
            reconcile_dust_tokens: env_dec("HFM_RECONCILE_DUST_TOKENS", dec!(0.000001))?,
            geyser_url: env_str("HFM_GEYSER_URL", ""),
            geyser_token: env_str("HFM_GEYSER_TOKEN", ""),
            feed_commitment: env_str("HFM_FEED_COMMITMENT", "processed"),
            max_trade_subs: env_u64("HFM_MAX_TRADE_SUBS", 25)? as usize,
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
        if self.paper_slippage_pct < Decimal::ZERO {
            return Err(format!(
                "HFM_PAPER_SLIPPAGE_PCT must be >= 0, got {}",
                self.paper_slippage_pct
            ));
        }
        if self.paper_impact_coeff < Decimal::ZERO {
            return Err(format!(
                "HFM_PAPER_IMPACT_COEFF must be >= 0, got {}",
                self.paper_impact_coeff
            ));
        }
        pos(self.paper_max_slippage_pct, "HFM_PAPER_MAX_SLIPPAGE_PCT")?;
        if self.paper_max_slippage_pct > dec!(100) {
            return Err(format!(
                "HFM_PAPER_MAX_SLIPPAGE_PCT must be <= 100, got {}",
                self.paper_max_slippage_pct
            ));
        }
        if self.heartbeat_secs == 0 {
            return Err("HFM_HEARTBEAT_SECS must be >= 1".into());
        }
        if self.metrics_addr.trim().is_empty() {
            return Err("HFM_METRICS_ADDR must not be empty".into());
        }
        if self.funnel_slices == 0 {
            return Err("HFM_FUNNEL_SLICES must be >= 1".into());
        }
        if self.max_open_positions == 0 {
            return Err("HFM_MAX_OPEN_POSITIONS must be >= 1".into());
        }
        if self.max_trades_per_min == 0 {
            return Err("HFM_MAX_TRADES_PER_MIN must be >= 1".into());
        }
        if self.max_exit_attempts == 0 {
            return Err("HFM_MAX_EXIT_ATTEMPTS must be >= 1".into());
        }
        if self.jupiter_qps == 0 {
            return Err("HFM_JUPITER_QPS must be >= 1".into());
        }
        if self.bundle_timeout_secs == 0 {
            return Err("HFM_BUNDLE_TIMEOUT_SECS must be >= 1".into());
        }
        if self.blockhash_ttl_secs == 0 {
            return Err("HFM_BLOCKHASH_TTL_SECS must be >= 1".into());
        }
        if self.reconcile_dust_tokens < Decimal::ZERO {
            return Err(format!(
                "HFM_RECONCILE_DUST_TOKENS must be >= 0, got {}",
                self.reconcile_dust_tokens
            ));
        }
        if self.max_trade_subs == 0 {
            return Err("HFM_MAX_TRADE_SUBS must be >= 1".into());
        }
        if self.sol_usd < Decimal::ZERO {
            return Err(format!("HFM_SOL_USD must be >= 0, got {}", self.sol_usd));
        }
        if self.jupiter_url.trim().is_empty() {
            return Err("HFM_JUPITER_URL must not be empty".into());
        }
        if self.rpc_url.trim().is_empty() {
            return Err("HFM_RPC_URL must not be empty".into());
        }
        if self.rpc_fallback_urls.iter().any(|u| u.trim().is_empty()) {
            return Err("HFM_RPC_FALLBACK_URLS must not contain empty entries".into());
        }
        for (i, acct) in self.jito_tip_accounts.iter().enumerate() {
            if solana_pubkey::Pubkey::from_str(acct).is_err() {
                return Err(format!(
                    "HFM_JITO_TIP_ACCOUNT entry {i} is not a valid base58 pubkey: '{acct}'"
                ));
            }
        }
        match self.feed_commitment.as_str() {
            "processed" | "confirmed" | "finalized" => {}
            other => {
                return Err(format!(
                    "HFM_FEED_COMMITMENT must be processed|confirmed|finalized, got '{other}'"
                ));
            }
        }
        // NOTE: HFM_TAKE_PROFIT_PCT < HFM_CONVICTION_MIN_PCT is valid and is the
        // spec default: climbers that pass +TP exit; only gap-movers that are
        // already past +conviction_min at a tick get promoted to trail mode.
        // M4: live mode is bootable, but layered safety stays on —
        // `LiveExecutor::armed` still requires an operator keypair, and
        // `simulate_only` defaults to true so an armed executor assembles and
        // signs without submitting until explicitly opted into sending.
        // (The old blanket refusal lived here pre-M4.)
        if self.mode == Mode::Live && self.sol_usd <= Decimal::ZERO {
            return Err(format!(
                "HFM_MODE=live requires HFM_SOL_USD > 0 (buy path converts USD to lamports), got {}",
                self.sol_usd
            ));
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

/// Blank means unset (falls back to the default): `.env.example` ships
/// optional knobs blank, and a blank value must behave exactly like a
/// missing one — never a boot error, never a silent zero.
fn env_dec(key: &str, default: Decimal) -> Result<Decimal, String> {
    match env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<Decimal>()
            .map_err(|e| format!("{key}: invalid decimal '{raw}': {e}")),
        _ => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("{key}: invalid integer '{raw}': {e}")),
        _ => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => match raw.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(format!("{key}: invalid bool '{other}'")),
        },
        _ => Ok(default),
    }
}

/// Comma-separated URL list (M6 RPC failover): whitespace-trimmed, empties
/// dropped, unset/blank = no entries. Never fails — validation rejects bad
/// entries loudly at boot instead.
fn env_csv(key: &str) -> Vec<String> {
    env::var(key)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
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
            max_exit_attempts: 5,
            conviction_min_pct: dec!(300),
            trail_pct: dec!(25),
            conviction_max_hold_secs: 15 * 86_400,
            daily_loss_limit_pct: dec!(10),
            max_trades_per_min: 6,
            kill_switch: false,
            paper_slippage_pct: dec!(2),
            paper_impact_coeff: dec!(1),
            paper_max_slippage_pct: dec!(50),
            fee_bps: 100,
            metrics_addr: "127.0.0.1:9898".into(),
            heartbeat_secs: 60,
            audit_log_path: "/tmp/hfmcbot-test-audit.jsonl".into(),
            replay_events_path: None,
            pg_dsn: String::new(),
            rpc_url: "https://example.invalid".into(),
            rpc_fallback_urls: Vec::new(),
            ws_url: "wss://example.invalid".into(),
            jito_url: "https://example.invalid".into(),
            jito_tip_lamports: 1_000_000,
            jito_tip_entry_lamports: 1_000_000,
            jito_tip_flip_lamports: 1_000_000,
            jito_tip_conviction_lamports: 1_000_000,
            jito_tip_accounts: Vec::new(),
            jupiter_url: "https://example.invalid".into(),
            jupiter_api_key: String::new(),
            jupiter_qps: 1,
            simulate_only: true,
            sol_usd: dec!(0),
            bundle_timeout_secs: 60,
            blockhash_ttl_secs: 30,
            reconcile_dust_tokens: dec!(0.000001),
            geyser_url: String::new(),
            geyser_token: String::new(),
            feed_commitment: "processed".into(),
            max_trade_subs: 25,
        }
    }
}
