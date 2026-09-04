//! Live execution pipeline (M3.5 scaffold → M4 devnet, spec §5): Jupiter Swap
//! API v1 quote + swap assembly, operator-keypair signing, Jito bundle
//! submission, and landing reconciliation.
//!
//! What works NOW (all unit-tested, network only against mocks or devnet):
//! - `TokenBucket` outbound governor (spec §6 rate limiting);
//! - Jupiter `/quote` URL builder + response parser + slippage-budget
//!   preflight (spec §4 slippage budget);
//! - USD→lamport and token-qty→raw-unit conversion (pure, Decimal-only);
//! - `/swap` request builder + response parser;
//! - versioned-transaction signing with payer check + fresh blockhash;
//! - Jito `sendBundle` + `getBundleStatuses` builders, status parser, and
//!   poll-until-landed reconciliation (timeout = unknown state, spec §5.5);
//! - `LiveExecutor`: refused unarmed in paper mode; armed in live mode with
//!   an operator keypair; `simulate_only` (default true) assembles and SIGNS
//!   real transactions but never submits them.
//!
//! What stays pre-mainnet: balance-based reconciliation (plus the blockhash
//! manager and Postgres audit called out in the roadmap).

use crate::config::{Config, Mode};
use crate::exec::{ExecError, Executor};
use crate::keys::LoadedKey;
use crate::types::{Fill, Side};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_message::{AccountMeta, Instruction, Message, VersionedMessage};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

/// SOL mint — the quote currency on the buy path (SOL → token) and the
/// proceeds currency on the sell path (token → SOL).
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Lamports per SOL. Integer only — money never touches floats.
pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

/// Integer token-bucket governor for outbound upstream calls (spec §6:
/// Jupiter + RPC must never see a burst). Whole-token accounting only —
/// fractional tokens are dropped, which can only *under*-admit traffic.
pub struct TokenBucket {
    capacity: u64,
    refill_per_sec: u64,
    available: u64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u64, refill_per_sec: u64) -> TokenBucket {
        TokenBucket {
            capacity,
            refill_per_sec,
            available: capacity,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let elapsed_nanos = self.last_refill.elapsed().as_nanos();
        if elapsed_nanos == 0 {
            return;
        }
        let earned = (elapsed_nanos * u128::from(self.refill_per_sec) / 1_000_000_000) as u64;
        if earned > 0 {
            self.available = self.available.saturating_add(earned).min(self.capacity);
            self.last_refill = Instant::now();
        }
    }

    /// Take `n` tokens if available. Pure burst check when called back to
    /// back (no time passes), which is exactly what the tests assert.
    pub fn try_take(&mut self, n: u64) -> bool {
        self.refill();
        if self.available >= n {
            self.available -= n;
            true
        } else {
            false
        }
    }
}

/// USD budget → lamports at the configured SOL/USD reference, rounded DOWN
/// (we never size up). Pure, Decimal-only, unit-tested.
pub fn usd_to_lamports_floor(budget_usd: Decimal, sol_usd: Decimal) -> Result<u64, ExecError> {
    if sol_usd <= Decimal::ZERO {
        return Err(ExecError::Rejected(format!(
            "no SOL/USD reference (got {sol_usd}) — set HFM_SOL_USD to arm live buys"
        )));
    }
    if budget_usd <= Decimal::ZERO {
        return Err(ExecError::Rejected(format!(
            "non-positive buy budget {budget_usd}"
        )));
    }
    let lamports = budget_usd / sol_usd * Decimal::from(LAMPORTS_PER_SOL);
    let v: u64 = lamports
        .floor()
        .to_string()
        .parse()
        .map_err(|_| ExecError::Rejected(format!("buy budget {budget_usd} overflows u64 lamports")))?;
    if v == 0 {
        return Err(ExecError::Rejected(format!(
            "buy budget {budget_usd} rounds to zero lamports"
        )));
    }
    Ok(v)
}

/// Lamports → USD at the SOL/USD reference. Pure.
pub fn lamports_to_usd(lamports: u64, sol_usd: Decimal) -> Decimal {
    Decimal::from(lamports) / Decimal::from(LAMPORTS_PER_SOL) * sol_usd
}

/// Raw base units → whole-token Decimal at `decimals`. Pure.
pub fn raw_to_decimal(raw: u64, decimals: u8) -> Decimal {
    Decimal::from(raw) / pow10(decimals)
}

/// Whole-token Decimal → raw base units, rounded DOWN. Pure.
pub fn decimal_to_raw_floor(qty: Decimal, decimals: u8) -> Result<u64, ExecError> {
    if qty <= Decimal::ZERO {
        return Err(ExecError::Rejected(format!("non-positive token qty {qty}")));
    }
    let raw = (qty * pow10(decimals)).floor();
    let v: u64 = raw
        .to_string()
        .parse()
        .map_err(|_| ExecError::Rejected(format!("token qty {qty} overflows u64 raw units")))?;
    if v == 0 {
        return Err(ExecError::Rejected(format!(
            "token qty {qty} rounds to zero raw units at {decimals} decimals"
        )));
    }
    Ok(v)
}

fn pow10(decimals: u8) -> Decimal {
    Decimal::from(10u64.pow(u32::from(decimals)))
}

/// Slippage percent → basis points for the Jupiter quote (rounded DOWN so we
/// never ask for MORE slippage than configured). Pure.
pub fn slippage_pct_to_bps(pct: Decimal) -> Result<u64, ExecError> {
    if pct < Decimal::ZERO {
        return Err(ExecError::Rejected(format!("negative slippage budget {pct}")));
    }
    (pct * dec!(100))
        .floor()
        .to_string()
        .parse::<u64>()
        .map_err(|_| ExecError::Rejected(format!("slippage budget {pct} overflows u64 bps")))
}
/// Parsed Jupiter Swap API v1 `/quote` response (amounts are RAW base units,
/// transmitted as strings; see https://api.jup.ag/swap/v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JupiterQuote {
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount_raw: u64,
    pub out_amount_raw: u64,
    pub other_amount_threshold_raw: u64,
    pub swap_mode: String,
    pub slippage_bps: u64,
    /// Signed decimal percent, e.g. "0.0001" (can be negative on weird routes).
    pub price_impact_pct: Decimal,
    /// The original quote JSON — `/swap` assembly takes the full quote
    /// response verbatim, so we carry it instead of re-fetching.
    pub raw: serde_json::Value,
}

/// Read-only Jupiter quote client. Holds no keys; quotes never move funds.
pub struct JupiterClient {
    base_url: String,
    api_key: Option<String>,
    http: reqwest::Client,
    bucket: TokenBucket,
}

impl JupiterClient {
    pub fn new(cfg: &Config) -> JupiterClient {
        JupiterClient {
            base_url: cfg.jupiter_url.trim_end_matches('/').to_string(),
            api_key: {
                let k = cfg.jupiter_api_key.trim().to_string();
                if k.is_empty() {
                    None
                } else {
                    Some(k)
                }
            },
            http: reqwest::Client::new(),
            // Burst covers one full quote+swap pass (2 calls); the sustained
            // rate stays qps. A burst of 1 would starve every pipeline's
            // second call by construction — the e2e tests pin this.
            bucket: TokenBucket::new(cfg.jupiter_qps as u64 * 2, cfg.jupiter_qps as u64),
        }
    }

    /// Pure URL builder (unit-tested): ExactIn quote with the funnel's
    /// slippage budget, restricted to Jito-compatible direct-ish routes.
    pub fn quote_url(
        base_url: &str,
        input_mint: &str,
        output_mint: &str,
        amount_raw: u64,
        slippage_bps: u64,
    ) -> String {
        format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&swapMode=ExactIn&onlyDirectRoutes=true&forJitoBundle=true&restrictIntermediateTokens=true",
            base_url.trim_end_matches('/'),
            input_mint,
            output_mint,
            amount_raw,
            slippage_bps,
        )
    }

    /// Fetch an ExactIn quote, governor-gated. Transport failures (HTTP,
    /// rate-limit exhaustion) surface as `Transport` — the caller must treat
    /// order state as unknown per spec §5.5.
    pub async fn quote(
        &mut self,
        input_mint: &str,
        output_mint: &str,
        amount_raw: u64,
        slippage_bps: u64,
    ) -> Result<JupiterQuote, ExecError> {
        if !self.bucket.try_take(1) {
            return Err(ExecError::Transport(
                "jupiter rate governor exhausted; quote skipped".into(),
            ));
        }
        let url = Self::quote_url(
            &self.base_url,
            input_mint,
            output_mint,
            amount_raw,
            slippage_bps,
        );
        let mut req = self.http.get(&url);
        if let Some(key) = &self.api_key {
            req = req.header("x-api-key", key);
        }
        let body = req
            .send()
            .await
            .map_err(|e| ExecError::Transport(format!("jupiter quote request failed: {e}")))?
            .error_for_status()
            .map_err(|e| ExecError::Transport(format!("jupiter quote HTTP error: {e}")))?
            .text()
            .await
            .map_err(|e| ExecError::Transport(format!("jupiter quote body unreadable: {e}")))?;
        parse_quote_response(&body)
    }

    /// Assemble a signed-ready swap transaction for a quote: POSTs the FULL
    /// quote response verbatim (Jupiter requires it) with the operator as fee
    /// payer. Returns base64 ready for local signing — the private key never
    /// leaves this process (spec §9).
    ///
    /// `prioritization_fee_lamports` is the M6 urgency tier expressed as a
    /// compute-budget fee (the OTHER half rides as the bundle's standalone
    /// tip transfer — see `build_tip_tx_b64`). 0 = no priority fee.
    pub async fn swap(
        &mut self,
        quote: &JupiterQuote,
        user_pubkey: &str,
        prioritization_fee_lamports: u64,
    ) -> Result<SwapTx, ExecError> {
        if !self.bucket.try_take(1) {
            return Err(ExecError::Transport(
                "jupiter rate governor exhausted; swap assembly skipped".into(),
            ));
        }
        let url = format!("{}/swap", self.base_url);
        let mut req = self.http.post(&url).json(&build_swap_request(
            &quote.raw,
            user_pubkey,
            prioritization_fee_lamports,
        ));
        if let Some(key) = &self.api_key {
            req = req.header("x-api-key", key);
        }
        let body = req
            .send()
            .await
            .map_err(|e| ExecError::Transport(format!("jupiter swap request failed: {e}")))?
            .error_for_status()
            .map_err(|e| ExecError::Transport(format!("jupiter swap HTTP error: {e}")))?
            .text()
            .await
            .map_err(|e| ExecError::Transport(format!("jupiter swap body unreadable: {e}")))?;
        parse_swap_response(&body)
    }
}

fn quote_field<'a>(v: &'a serde_json::Value, name: &str) -> Result<&'a str, ExecError> {
    v.get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ExecError::Rejected(format!("jupiter quote missing/invalid '{name}'")))
}

fn quote_u64(v: &serde_json::Value, name: &str) -> Result<u64, ExecError> {
    quote_field(v, name)?
        .parse::<u64>()
        .map_err(|_| ExecError::Rejected(format!("jupiter quote '{name}' not a u64")))
}

/// Pure parser for the Jupiter `/quote` JSON body (unit-tested against a
/// realistic response shape). Anything malformed is a `Rejected` quote —
/// we refuse to trade on a quote we cannot read, never guess.
pub fn parse_quote_response(body: &str) -> Result<JupiterQuote, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("jupiter quote not JSON: {e}")))?;
    let slippage_bps = v
        .get("slippageBps")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ExecError::Rejected("jupiter quote missing/invalid 'slippageBps'".into()))?;
    let price_impact_pct = quote_field(&v, "priceImpactPct")?
        .parse::<Decimal>()
        .map_err(|_| ExecError::Rejected("jupiter quote 'priceImpactPct' not decimal".into()))?;
    Ok(JupiterQuote {
        input_mint: quote_field(&v, "inputMint")?.to_string(),
        output_mint: quote_field(&v, "outputMint")?.to_string(),
        in_amount_raw: quote_u64(&v, "inAmount")?,
        out_amount_raw: quote_u64(&v, "outAmount")?,
        other_amount_threshold_raw: quote_u64(&v, "otherAmountThreshold")?,
        swap_mode: quote_field(&v, "swapMode")?.to_string(),
        slippage_bps,
        price_impact_pct,
        raw: v,
    })
}

/// Assembled-but-unsigned swap transaction from Jupiter `/swap`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapTx {
    /// Base64 bincode-serialized `VersionedTransaction` with a placeholder
    /// signature — must be locally signed before submission.
    pub tx_base64: String,
    pub last_valid_block_height: u64,
}

/// Pure `/swap` request builder (unit-tested). The quote response goes in
/// verbatim; SOL wrapping is on (buys pay in SOL, sells receive SOL);
/// compute units are dynamic so funnel slices land under load. The urgency
/// tier rides as `prioritizationFeeLamports` (numeric lamports) — one half
/// of the M6 urgency price; the other half is the bundle's standalone tip
/// transfer (`build_tip_tx_b64`), and the accounted fill fee covers both.
pub fn build_swap_request(
    quote_raw: &serde_json::Value,
    user_pubkey: &str,
    prioritization_fee_lamports: u64,
) -> serde_json::Value {
    serde_json::json!({
        "quoteResponse": quote_raw,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        "prioritizationFeeLamports": prioritization_fee_lamports,
    })
}

/// Pure parser for the Jupiter `/swap` JSON body. Malformed = `Rejected` —
/// we refuse to sign bytes we cannot read, never guess.
pub fn parse_swap_response(body: &str) -> Result<SwapTx, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("jupiter swap not JSON: {e}")))?;
    let tx_base64 = v
        .get("swapTransaction")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExecError::Rejected("jupiter swap missing/invalid 'swapTransaction'".into()))?;
    let last_valid_block_height = v
        .get("lastValidBlockHeight")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ExecError::Rejected("jupiter swap missing/invalid 'lastValidBlockHeight'".into())
        })?;
    Ok(SwapTx {
        tx_base64: tx_base64.to_string(),
        last_valid_block_height,
    })
}
/// Spec §4 slippage budget: a quote whose `priceImpactPct` exceeds the
/// configured max is refused BEFORE any swap assembly. Pure, unit-tested.
pub fn quote_within_budget(
    quote: &JupiterQuote,
    max_slippage_pct: Decimal,
) -> Result<(), ExecError> {
    if quote.price_impact_pct.abs() > max_slippage_pct {
        return Err(ExecError::Rejected(format!(
            "jupiter price impact {}% exceeds budget {}%",
            quote.price_impact_pct, max_slippage_pct
        )));
    }
    Ok(())
}

/// Read-only Solana RPC client for live-execution plumbing (spec §5.2):
/// fresh blockhashes for signing (never reuse, spec §6) and mint decimals
/// for raw-unit conversion. No keys, no sends — pure reads.
///
/// M6 multi-RPC failover: the primary (`HFM_RPC_URL`) plus ordered fallbacks
/// (`HFM_RPC_FALLBACK_URLS`) are tried in rotation starting from the last
/// known-good endpoint (sticky-success, so a dead primary is not hammered on
/// every call). Failover triggers on TRANSPORT errors only — a reachable
/// endpoint returning unparsable data is a `Rejected` bug, not a reason to
/// shop the same question to another server and possibly act on a different
/// answer.
pub struct RpcClient {
    urls: Vec<String>,
    http: reqwest::Client,
    /// Index of the last known-good endpoint. Sticky on success, advanced
    /// past failures; every call still starts here and wraps around.
    active: std::sync::atomic::AtomicUsize,
}

impl RpcClient {
    pub fn new(cfg: &Config) -> RpcClient {
        let mut urls = vec![cfg.rpc_url.trim_end_matches('/').to_string()];
        urls.extend(
            cfg.rpc_fallback_urls
                .iter()
                .map(|u| u.trim_end_matches('/').to_string()),
        );
        RpcClient::with_urls(urls)
    }

    fn with_urls(urls: Vec<String>) -> RpcClient {
        RpcClient {
            urls,
            // A hung primary must not stall the hot path forever: 15s per
            // attempt, then the next endpoint gets its turn.
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("reqwest client with timeout builds"),
            active: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Currently preferred endpoint (the last one that answered).
    pub fn active_url(&self) -> &str {
        let i = self
            .active
            .load(std::sync::atomic::Ordering::SeqCst)
            .min(self.urls.len().saturating_sub(1));
        &self.urls[i]
    }

    #[cfg(test)]
    fn new_for_test(base_url: &str) -> RpcClient {
        RpcClient::with_urls(vec![base_url.trim_end_matches('/').to_string()])
    }

    #[cfg(test)]
    fn new_for_test_many(urls: Vec<String>) -> RpcClient {
        RpcClient::with_urls(urls)
    }

    async fn post_one(
        &self,
        url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<String, ExecError> {
        let body = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|e| ExecError::Transport(format!("rpc {method} request failed: {e}")))?
            .error_for_status()
            .map_err(|e| ExecError::Transport(format!("rpc {method} HTTP error: {e}")))?
            .text()
            .await
            .map_err(|e| ExecError::Transport(format!("rpc {method} body unreadable: {e}")))?;
        Ok(body)
    }

    async fn rpc_call(&self, method: &str, params: serde_json::Value) -> Result<String, ExecError> {
        use std::sync::atomic::Ordering;
        let n = self.urls.len();
        let start = self.active.load(Ordering::SeqCst).min(n.saturating_sub(1));
        let mut last_err = ExecError::Transport("rpc: no endpoints configured".into());
        for k in 0..n {
            let i = (start + k) % n;
            match self
                .post_one(&self.urls[i].clone(), method, params.clone())
                .await
            {
                Ok(body) => {
                    if k > 0 {
                        tracing::warn!(
                            url = %self.urls[i],
                            method,
                            attempts = k + 1,
                            "rpc failover: recovered on fallback endpoint"
                        );
                    }
                    self.active.store(i, Ordering::SeqCst);
                    return Ok(body);
                }
                Err(e) => {
                    tracing::warn!(
                        url = %self.urls[i],
                        method,
                        error = %e,
                        "rpc endpoint failed, trying next"
                    );
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    /// Fresh blockhash for signing. Callers fetch one per bundle — blockhash
    /// reuse across bundles is a spec §6 violation.
    pub async fn fetch_recent_blockhash(&self) -> Result<(String, u64), ExecError> {
        let body = self
            .rpc_call(
                "getLatestBlockhash",
                serde_json::json!([{"commitment": "finalized"}]),
            )
            .await?;
        parse_blockhash_response(&body)
    }

    /// Mint decimals for raw-unit conversion (sells size in tokens, Jupiter
    /// quotes in raw units). Unknown mint = `Rejected`, never guessed.
    pub async fn fetch_token_decimals(&self, mint: &str) -> Result<u8, ExecError> {
        let body = self
            .rpc_call("getTokenSupply", serde_json::json!([mint]))
            .await?;
        parse_token_supply_decimals(&body)
    }

    /// Operator-owned token balance for `mint` (sum over all token accounts,
    /// exact Decimal — never floats). The post-landing reconciliation read:
    /// after a bundle lands, the wallet MUST show what the fill claims.
    pub async fn fetch_token_balance(
        &self,
        owner: &str,
        mint: &str,
    ) -> Result<Decimal, ExecError> {
        let body = self
            .rpc_call(
                "getTokenAccountsByOwner",
                serde_json::json!([owner, {"mint": mint}, {"encoding": "jsonParsed"}]),
            )
            .await?;
        parse_token_accounts_balance(&body)
    }

    /// Devnet/funding plumbing (also the future direct-send fallback path):
    /// airdrop, SOL balance, raw transaction submission, and signature
    /// confirmation. Submission returning a signature is NOT landing —
    /// callers must `confirm_signature` (spec §5.5: unknown until confirmed).
    pub async fn request_airdrop(&self, pubkey: &str, lamports: u64) -> Result<String, ExecError> {
        let body = self
            .rpc_call("requestAirdrop", serde_json::json!([pubkey, lamports]))
            .await?;
        parse_string_result(&body, "airdrop")
    }

    pub async fn get_balance(&self, pubkey: &str) -> Result<u64, ExecError> {
        let body = self
            .rpc_call("getBalance", serde_json::json!([pubkey]))
            .await?;
        parse_balance_response(&body)
    }

    pub async fn send_transaction(&self, tx_b64: &str) -> Result<String, ExecError> {
        let body = self
            .rpc_call(
                "sendTransaction",
                serde_json::json!([tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}]),
            )
            .await?;
        parse_string_result(&body, "sendTransaction")
    }

    /// Poll `getSignatureStatuses` until confirmed/finalized, failed, or the
    /// timeout expires. Timeout is TRANSPORT (unknown) — never assume.
    pub async fn confirm_signature(&self, sig: &str, timeout_secs: u64) -> Result<(), ExecError> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            let body = self
                .rpc_call(
                    "getSignatureStatuses",
                    serde_json::json!([[sig], {"searchTransactionHistory": false}]),
                )
                .await?;
            match parse_sig_status(&body)? {
                SigStatus::Confirmed => return Ok(()),
                SigStatus::Failed(err) => {
                    return Err(ExecError::Rejected(format!(
                        "transaction {sig} failed on-chain: {err}"
                    )));
                }
                SigStatus::Pending => {}
            }
            if Instant::now() >= deadline {
                return Err(ExecError::Transport(format!(
                    "signature {sig} unconfirmed after {timeout_secs}s — reconcile before re-issuing"
                )));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Pure parser for RPC methods whose `result` is a plain string
/// (`requestAirdrop` → signature, `sendTransaction` → signature). An RPC-level
/// `error` object is `Rejected` (the node refused — definitive); unparseable
/// shapes are `Rejected` at the parse layer, never Transport.
pub fn parse_string_result(body: &str, method: &str) -> Result<String, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc {method} not JSON: {e}")))?;
    if let Some(err) = v.get("error") {
        return Err(ExecError::Rejected(format!("rpc {method} refused: {err}")));
    }
    v.get("result")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExecError::Rejected(format!("rpc {method} missing string 'result'")))
}

/// Pure parser for `getBalance` → lamports.
pub fn parse_balance_response(body: &str) -> Result<u64, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc balance not JSON: {e}")))?;
    v.get("result")
        .and_then(|r| r.get("value"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ExecError::Rejected("rpc balance missing 'result.value'".into()))
}

/// Confirmation state of one signature from `getSignatureStatuses`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigStatus {
    Confirmed,
    Failed(String),
    /// `null` entry (unknown to the node yet) or unrecognized shape — keep
    /// waiting while time remains, never success.
    Pending,
}

/// Pure parser for `getSignatureStatuses` → status of the first signature.
/// Only explicit `confirmationStatus` finalized/confirmed lands; only an
/// explicit `err` (non-null) fails; everything else is Pending.
pub fn parse_sig_status(body: &str) -> Result<SigStatus, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc sig status not JSON: {e}")))?;
    let entry = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(serde_json::Value::as_array)
        .and_then(|a| a.first())
        .ok_or_else(|| ExecError::Rejected("rpc sig status missing 'result.value[0]'".into()))?;
    if entry.is_null() {
        return Ok(SigStatus::Pending);
    }
    if let Some(err) = entry.get("err") {
        if err.is_null() {
            // err: null with no status yet — still pending, not confirmed.
        } else if err.get("Ok").is_some() {
            // Explicit Ok — fall through to status check below.
        } else {
            return Ok(SigStatus::Failed(err.to_string()));
        }
    }
    match entry
        .get("confirmationStatus")
        .and_then(serde_json::Value::as_str)
    {
        Some("finalized") | Some("confirmed") => Ok(SigStatus::Confirmed),
        _ => Ok(SigStatus::Pending),
    }
}

/// Pure parser for `getTokenAccountsByOwner` (jsonParsed) → summed balance.
///
/// Prefers `uiAmountString` (exact decimal string); falls back to
/// `amount` + `decimals` raw conversion. Never touches `uiAmount` (f64 —
/// float dust has no place in reconciliation). Missing/unparseable entries
/// are `Rejected`: a balance we cannot read exactly is not a balance.
pub fn parse_token_accounts_balance(body: &str) -> Result<Decimal, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc token accounts not JSON: {e}")))?;
    let arr = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ExecError::Rejected("rpc token accounts missing 'result.value'".into()))?;
    let mut total = Decimal::ZERO;
    for entry in arr {
        let amount = entry
            .get("account")
            .and_then(|a| a.get("data"))
            .and_then(|d| d.get("parsed"))
            .and_then(|p| p.get("info"))
            .and_then(|i| i.get("tokenAmount"))
            .ok_or_else(|| {
                ExecError::Rejected("rpc token accounts entry missing tokenAmount".into())
            })?;
        if let Some(s) = amount
            .get("uiAmountString")
            .and_then(serde_json::Value::as_str)
        {
            total += s.parse::<Decimal>().map_err(|_| {
                ExecError::Rejected(format!("rpc token accounts bad uiAmountString '{s}'"))
            })?;
        } else {
            let raw = amount
                .get("amount")
                .and_then(serde_json::Value::as_str)
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| {
                    ExecError::Rejected("rpc token accounts entry missing amount".into())
                })?;
            let decimals = amount
                .get("decimals")
                .and_then(serde_json::Value::as_u64)
                .and_then(|d| u8::try_from(d).ok())
                .ok_or_else(|| {
                    ExecError::Rejected("rpc token accounts entry missing decimals".into())
                })?;
            total += raw_to_decimal(raw, decimals);
        }
    }
    Ok(total)
}

/// Pure parser for `getLatestBlockhash` → (blockhash, lastValidBlockHeight).
pub fn parse_blockhash_response(body: &str) -> Result<(String, u64), ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc blockhash not JSON: {e}")))?;
    let value = v
        .get("result")
        .and_then(|r| r.get("value"))
        .ok_or_else(|| ExecError::Rejected("rpc blockhash missing 'result.value'".into()))?;
    let blockhash = value
        .get("blockhash")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExecError::Rejected("rpc blockhash missing/invalid 'blockhash'".into()))?;
    // Fail fast on undecodable blockhashes — signing would fail anyway, and
    // an early refuse keeps the error at the parsing layer, unit-testable.
    Hash::from_str(blockhash)
        .map_err(|_| ExecError::Rejected("rpc blockhash not valid base58 hash".into()))?;
    let height = value
        .get("lastValidBlockHeight")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ExecError::Rejected("rpc blockhash missing/invalid 'lastValidBlockHeight'".into())
        })?;
    Ok((blockhash.to_string(), height))
}

/// Pure parser for `getTokenSupply` → decimals.
pub fn parse_token_supply_decimals(body: &str) -> Result<u8, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Rejected(format!("rpc token supply not JSON: {e}")))?;
    v.get("result")
        .and_then(|r| r.get("value"))
        .and_then(|val| val.get("decimals"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|d| u8::try_from(d).ok())
        .ok_or_else(|| ExecError::Rejected("rpc token supply missing/invalid 'decimals'".into()))
}

/// Locally sign an assembled Jupiter swap transaction (spec §5.1 + §9):
/// base64 in, base64 out. The secret never leaves this process.
///
/// Safety order is deliberate:
/// 1. decode + deserialize (malformed bytes = `Rejected`);
/// 2. **payer check** — the tx's fee payer must BE the operator; we refuse to
///    sign a transaction that spends anyone else's authority;
/// 3. stamp a FRESH blockhash (never reuse, spec §6);
/// 4. re-sign from scratch with the operator keypair (`try_new` fills exactly
///    the required slots — Jupiter user-txs need only the operator; anything
///    demanding co-signers we don't hold is refused, never half-signed).
pub fn sign_versioned_tx_b64(
    tx_b64: &str,
    keypair: &Keypair,
    blockhash: &str,
) -> Result<String, ExecError> {
    use base64::Engine as _;
    let bytes = base64::prelude::BASE64_STANDARD
        .decode(tx_b64.trim())
        .map_err(|e| ExecError::Rejected(format!("swap tx not base64: {e}")))?;
    let tx: VersionedTransaction = bincode::deserialize(&bytes)
        .map_err(|e| ExecError::Rejected(format!("swap tx not a versioned transaction: {e}")))?;
    let payer = tx
        .message
        .static_account_keys()
        .first()
        .ok_or_else(|| ExecError::Rejected("swap tx has no accounts".into()))?;
    if payer != &keypair.pubkey() {
        return Err(ExecError::Rejected(
            "refusing to sign: tx fee payer is not the operator keypair".into(),
        ));
    }
    let fresh = Hash::from_str(blockhash)
        .map_err(|_| ExecError::Rejected("refusing to sign: blockhash not valid".into()))?;
    let mut message = tx.message;
    match &mut message {
        VersionedMessage::Legacy(msg) => msg.recent_blockhash = fresh,
        VersionedMessage::V0(msg) => msg.recent_blockhash = fresh,
    }
    let signed = VersionedTransaction::try_new(message, &[keypair])
        .map_err(|e| ExecError::Rejected(format!("signing failed: {e}")))?;
    let bytes = bincode::serialize(&signed)
        .map_err(|e| ExecError::Rejected(format!("signed tx unserializable: {e}")))?;
    Ok(base64::prelude::BASE64_STANDARD.encode(bytes))
}

/// System program id (all-ones): the transfer instruction's program. A frozen
/// protocol constant, kept as one named const — never inline magic.
pub const SYSTEM_PROGRAM_ID_STR: &str = "11111111111111111111111111111111";

/// SystemProgram `Transfer` layout: u32 LE discriminator `2`, then u64 LE
/// lamports. Hand-rolled (no extra dep) because the layout is a frozen
/// protocol constant — and every byte is asserted by the tests below.
const SYSTEM_TRANSFER_DISCRIMINATOR: u32 = 2;

/// Parse one `HFM_JITO_TIP_ACCOUNT` entry. Fail-closed: garbage is `Rejected`,
/// never skipped — a bundle tipping the wrong destination is worse than no
/// bundle. (Tip accounts are Jito-published public keys — safe to echo.)
pub fn parse_tip_account(s: &str) -> Result<Pubkey, ExecError> {
    Pubkey::from_str(s.trim()).map_err(|_| {
        ExecError::Rejected(format!("jito tip account not a valid base58 pubkey: '{s}'"))
    })
}

/// Round-robin tip-account selection across bundles: spreads tips over the
/// operator's listed accounts instead of hammering one. Pure.
pub fn pick_tip_account(accounts: &[Pubkey], counter: u64) -> Option<Pubkey> {
    if accounts.is_empty() {
        return None;
    }
    Some(accounts[(counter as usize) % accounts.len()])
}

/// Build the SystemProgram transfer instruction operator → tip account.
/// Pure: program id, account flags (from = signer+writable, to = writable),
/// and data layout are all asserted by tests.
pub fn build_tip_transfer_ix(
    from: &Pubkey,
    to: &Pubkey,
    lamports: u64,
) -> Result<Instruction, ExecError> {
    let program_id = Pubkey::from_str(SYSTEM_PROGRAM_ID_STR)
        .map_err(|e| ExecError::Rejected(format!("system program id unparseable: {e}")))?;
    let mut data = SYSTEM_TRANSFER_DISCRIMINATOR.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    Ok(Instruction {
        program_id,
        accounts: vec![AccountMeta::new(*from, true), AccountMeta::new(*to, false)],
        data,
    })
}

/// Assemble + sign the standalone tip transaction: SystemProgram transfer
/// operator → tip account for the tier's lamports, legacy message, caller-
/// supplied fresh blockhash (the SAME blockhash as the swap — one fetch per
/// bundle), operator-signed. Base64 out, bundle-ready.
///
/// A SEPARATE transaction (never an ix appended to the Jupiter swap) on
/// purpose: Jupiter's assembled tx is opaque — we do no surgery on bytes we
/// didn't build. Bundle order is swap-first, tip-second. Zero lamports is
/// `Rejected` (callers skip the tip tx instead; a 0-lamport transfer would
/// waste block space and lie in accounting).
pub fn build_tip_tx_b64(
    operator: &Keypair,
    tip_account: &Pubkey,
    lamports: u64,
    blockhash: &str,
) -> Result<String, ExecError> {
    use base64::Engine as _;
    if lamports == 0 {
        return Err(ExecError::Rejected(
            "refusing to build a zero-lamport tip transfer".into(),
        ));
    }
    let ix = build_tip_transfer_ix(&operator.pubkey(), tip_account, lamports)?;
    let fresh = Hash::from_str(blockhash)
        .map_err(|_| ExecError::Rejected("refusing to build tip tx: blockhash not valid".into()))?;
    let mut message = Message::new(&[ix], Some(&operator.pubkey()));
    message.recent_blockhash = fresh;
    let signed = VersionedTransaction::try_new(VersionedMessage::Legacy(message), &[operator])
        .map_err(|e| ExecError::Rejected(format!("tip tx signing failed: {e}")))?;
    let bytes = bincode::serialize(&signed)
        .map_err(|e| ExecError::Rejected(format!("tip tx unserializable: {e}")))?;
    Ok(base64::prelude::BASE64_STANDARD.encode(bytes))
}

/// Bundle assembly (pure): swap first, tip second. No tip (a tier the
/// operator priced at zero) = swap-only bundle.
pub fn assemble_bundle(swap_b64: String, tip_b64: Option<String>) -> Vec<String> {
    match tip_b64 {
        Some(t) => vec![swap_b64, t],
        None => vec![swap_b64],
    }
}

/// Thin Jito block-engine client. Each bundle carries the swap plus a
/// standalone SystemProgram tip transfer (assembled at signing time); this
/// client only submits fully-signed base64 transactions via JSON-RPC
/// `sendBundle`.
pub struct JitoClient {
    base_url: String,
    /// Per-tier lamport tips (M6): flip stops outbid conviction trails
    /// outbid entries. See `tip_for`.
    pub tips: TipSchedule,
    /// Tip destination accounts from `HFM_JITO_TIP_ACCOUNT` (raw strings;
    /// parsed + validated at send time so a bad entry fails the ORDER, not
    /// the boot — and tests can drive the fail-closed path directly).
    pub tip_accounts: Vec<String>,
    http: reqwest::Client,
}

/// Urgency-priced tip schedule. Each tier is independently configurable;
/// all default to `HFM_JITO_TIP_LAMPORTS` so a single knob still prices
/// everything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipSchedule {
    pub entry: u64,
    pub flip: u64,
    pub conviction: u64,
}

impl JitoClient {
    pub fn new(cfg: &Config) -> JitoClient {
        JitoClient {
            base_url: cfg.jito_url.trim_end_matches('/').to_string(),
            tips: TipSchedule {
                entry: cfg.jito_tip_entry_lamports,
                flip: cfg.jito_tip_flip_lamports,
                conviction: cfg.jito_tip_conviction_lamports,
            },
            tip_accounts: cfg.jito_tip_accounts.clone(),
            http: reqwest::Client::new(),
        }
    }

    /// Lamport tip for an urgency tier. Pure — the selection policy is
    /// unit-testable without a keypair or network.
    pub fn tip_for(&self, tier: crate::exec::TipTier) -> u64 {
        match tier {
            crate::exec::TipTier::Entry => self.tips.entry,
            crate::exec::TipTier::FlipExit => self.tips.flip,
            crate::exec::TipTier::ConvictionExit => self.tips.conviction,
        }
    }

    /// Pure `sendBundle` request builder (unit-tested): params are
    /// `[ [txs...], {"encoding":"base64"} ]` per the block-engine API.
    pub fn build_send_bundle_request(id: u64, base64_txs: &[String]) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "sendBundle",
            "params": [base64_txs, {"encoding": "base64"}],
        })
    }

    /// Submit a signed bundle; returns the bundle id on acceptance.
    /// Acceptance ≠ landing — callers must reconcile via
    /// `poll_bundle_landing` (spec §5.5).
    pub async fn send_bundle(&self, id: u64, base64_txs: &[String]) -> Result<String, ExecError> {
        let url = format!("{}/api/v1/bundles", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&Self::build_send_bundle_request(id, base64_txs))
            .send()
            .await
            .map_err(|e| ExecError::Transport(format!("jito bundle request failed: {e}")))?
            .error_for_status()
            .map_err(|e| ExecError::Transport(format!("jito bundle HTTP error: {e}")))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ExecError::Transport(format!("jito bundle response unreadable: {e}")))?;
        resp.get("result")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ExecError::Transport(format!("jito bundle rejected: {resp}")))
    }

    /// Query bundle status by id. Transport errors here mean the state is
    /// unknown — the poll loop treats them as "keep waiting", never as
    /// success or failure.
    pub async fn get_bundle_statuses(
        &self,
        id: u64,
        bundle_uuid: &str,
    ) -> Result<BundleStatus, ExecError> {
        let url = format!("{}/api/v1/bundles", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&Self::build_get_bundle_statuses_request(
                id,
                &[bundle_uuid.to_string()],
            ))
            .send()
            .await
            .map_err(|e| ExecError::Transport(format!("jito status request failed: {e}")))?
            .error_for_status()
            .map_err(|e| ExecError::Transport(format!("jito status HTTP error: {e}")))?
            .text()
            .await
            .map_err(|e| ExecError::Transport(format!("jito status body unreadable: {e}")))?;
        parse_bundle_status(&resp)
    }

    /// Landing reconciliation (spec §5.5): poll until the bundle lands,
    /// fails, or the timeout expires. Returns landed transaction signatures.
    /// Timeout is TRANSPORT (unknown state) — the stuck-position path
    /// reconciles later; it must never blind-resubmit.
    pub async fn poll_bundle_landing(
        &self,
        bundle_uuid: &str,
        timeout_secs: u64,
    ) -> Result<Vec<String>, ExecError> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut id = 1u64;
        loop {
            match self.get_bundle_statuses(id, bundle_uuid).await {
                Ok(BundleStatus::Landed { signatures }) => return Ok(signatures),
                Ok(BundleStatus::Failed { reason }) => {
                    return Err(ExecError::Rejected(format!(
                        "bundle {bundle_uuid} failed on-chain: {reason}"
                    )));
                }
                // Pending OR transport blip: keep waiting while time remains.
                Ok(BundleStatus::Pending) | Err(ExecError::Transport(_)) => {}
                Err(other) => return Err(other),
            }
            if Instant::now() >= deadline {
                return Err(ExecError::Transport(format!(
                    "bundle {bundle_uuid} landing unknown after {timeout_secs}s — reconcile before re-issuing"
                )));
            }
            id = id.wrapping_add(1);
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(remaining.min(Duration::from_secs(2))).await;
        }
    }
}

/// Bundle landing state from `getBundleStatuses`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleStatus {
    /// Landed with the bundle's transaction signatures (public info — safe
    /// to log and audit).
    Landed { signatures: Vec<String> },
    /// Definitively failed on-chain (replayable error string from Jito).
    Failed { reason: String },
    /// Not yet landed (or not yet visible) — keep polling.
    Pending,
}

/// JitoClient also exposes the statuses builder as an associated fn so both
/// builders live next to their caller (mirrors `build_send_bundle_request`).
impl JitoClient {
    /// Pure `getBundleStatuses` request builder (unit-tested): params are
    /// `[ [uuids...] ]` per the block-engine API.
    pub fn build_get_bundle_statuses_request(id: u64, uuids: &[String]) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "getBundleStatuses",
            "params": [uuids],
        })
    }
}

/// Pure parser for `getBundleStatuses` → `BundleStatus` (unit-tested).
/// Unknown/empty shapes are `Pending` (keep waiting), never success —
/// only an explicit finalized/confirmed status with `Ok` lands, and only an
/// explicit `Err` fails.
pub fn parse_bundle_status(body: &str) -> Result<BundleStatus, ExecError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| ExecError::Transport(format!("jito status not JSON: {e}")))?;
    let entries = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(serde_json::Value::as_array);
    let entry = match entries.and_then(|a| a.first()) {
        Some(e) => e,
        None => return Ok(BundleStatus::Pending),
    };
    if let Some(err) = entry.get("err") {
        let failed = !err.get("Ok").is_some();
        if failed {
            return Ok(BundleStatus::Failed {
                reason: err.to_string(),
            });
        }
    }
    let status = entry
        .get("confirmation_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("pending");
    match status {
        "finalized" | "confirmed" => {
            let signatures = entry
                .get("transactions")
                .and_then(serde_json::Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            Ok(BundleStatus::Landed { signatures })
        }
        _ => Ok(BundleStatus::Pending),
    }
}

/// Live executor (M4): same `Executor` interface the engine already drives.
/// Three states:
///
/// - **unarmed** (paper mode): every order is refused with `NotArmed` —
///   construction is total, and using it can never move funds;
/// - **armed, keyless** (`armed` with a pubkey string): enforces the funnel
///   deadline (D11, real logic, tested) but refuses at signing — a pubkey
///   alone can never authorize a transaction;
/// - **armed with signer** (`armed_with_signer`): the full M4 pipeline —
///   quote → preflight → swap assembly → local signing → (unless
///   `simulate_only`) Jito bundle → landing reconciliation. No engine
///   changes were needed: the trait boundary held.
pub struct LiveExecutor {
    operator_pubkey: String,
    /// Raw 64-byte secret, reconstructed into a `Keypair` only at signing
    /// time. `None` until `armed_with_signer` — never logged, never sent.
    signer_bytes: Option<[u8; 64]>,
    jupiter: JupiterClient,
    jito: JitoClient,
    rpc: RpcClient,
    max_slippage_pct: Decimal,
    sol_usd: Decimal,
    simulate_only: bool,
    bundle_timeout_secs: u64,
    /// Blockhash manager (roadmap: no thundering fetch per bundle, no stale
    /// signing): last blockhash + fetch time; reused while younger than
    /// `blockhash_ttl_secs`. Reuse across DISTINCT transactions is safe (a
    /// blockhash only proves recency) — the TTL bounds staleness.
    cached_blockhash: Option<(String, u64, Instant)>,
    blockhash_ttl_secs: u64,
    /// Dust tolerance for post-landing balance reconciliation: exact-match
    /// accounting down to the raw unit is a fantasy (rent, prior holdings,
    /// route dust), so `±dust` passes. Anything beyond is UNKNOWN state.
    reconcile_dust_tokens: Decimal,
    req_id: u64,
    armed: bool,
}

impl LiveExecutor {
    /// Paper-safe constructor: always unarmed, never touches the network.
    pub fn unarmed_for_paper(cfg: &Config, operator_pubkey: &str) -> LiveExecutor {
        LiveExecutor {
            operator_pubkey: operator_pubkey.to_string(),
            signer_bytes: None,
            jupiter: JupiterClient::new(cfg),
            jito: JitoClient::new(cfg),
            rpc: RpcClient::new(cfg),
            max_slippage_pct: cfg.max_slippage_pct,
            sol_usd: cfg.sol_usd,
            simulate_only: true,
            bundle_timeout_secs: cfg.bundle_timeout_secs,
            cached_blockhash: None,
            blockhash_ttl_secs: cfg.blockhash_ttl_secs,
            reconcile_dust_tokens: cfg.reconcile_dust_tokens,
            req_id: 1,
            armed: false,
        }
    }

    /// Live-mode constructor: refuses to arm unless the config is really
    /// in live mode AND an operator pubkey is present. Keyless: enforces the
    /// funnel deadline but cannot sign.
    pub fn armed(cfg: &Config, operator_pubkey: &str) -> Result<LiveExecutor, ExecError> {
        if cfg.mode != Mode::Live {
            return Err(ExecError::NotArmed(
                "refuses to arm in paper mode — use PaperExecutor".into(),
            ));
        }
        if operator_pubkey.trim().is_empty() {
            return Err(ExecError::NotArmed(
                "no operator pubkey — set HFM_SECRET_KEY first".into(),
            ));
        }
        Ok(LiveExecutor {
            operator_pubkey: operator_pubkey.to_string(),
            signer_bytes: None,
            jupiter: JupiterClient::new(cfg),
            jito: JitoClient::new(cfg),
            rpc: RpcClient::new(cfg),
            max_slippage_pct: cfg.max_slippage_pct,
            sol_usd: cfg.sol_usd,
            simulate_only: cfg.simulate_only,
            bundle_timeout_secs: cfg.bundle_timeout_secs,
            cached_blockhash: None,
            blockhash_ttl_secs: cfg.blockhash_ttl_secs,
            reconcile_dust_tokens: cfg.reconcile_dust_tokens,
            req_id: 1,
            armed: true,
        })
    }

    /// Full M4 constructor: arms AND loads the signing key. Fail-fast: live
    /// mode + a SOL/USD reference are both required — the buy path cannot
    /// convert USD→lamports without one.
    pub fn armed_with_signer(cfg: &Config, loaded: &LoadedKey) -> Result<LiveExecutor, ExecError> {
        let mut ex = Self::armed(cfg, &loaded.pubkey_base58)?;
        if cfg.sol_usd <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "HFM_SOL_USD must be > 0 to arm live buys, got {}",
                cfg.sol_usd
            )));
        }
        ex.signer_bytes = Some(loaded.keypair.to_bytes());
        Ok(ex)
    }

    pub fn is_armed(&self) -> bool {
        self.armed
    }

    /// One-line operator summary for boot logs. Reads every client field so
    /// the scaffolding stays warning-clean. Never includes key material.
    pub fn describe(&self) -> String {
        format!(
            "live executor (operator {}, armed {}, signer {}, jupiter {}, jito tips entry/flip/conviction {}/{}/{} lamports to {} account(s), slippage budget {}%, simulate_only {})",
            self.operator_pubkey,
            self.armed,
            self.signer_bytes.is_some(),
            self.jupiter.base_url,
            self.jito.tips.entry,
            self.jito.tips.flip,
            self.jito.tips.conviction,
            self.jito.tip_accounts.len(),
            self.max_slippage_pct,
            self.simulate_only,
        )
    }

    fn next_id(&mut self) -> u64 {
        let id = self.req_id;
        self.req_id = self.req_id.wrapping_add(1);
        id
    }

    fn signer(&self) -> Result<Keypair, ExecError> {
        let bytes = self.signer_bytes.as_ref().ok_or_else(|| {
            ExecError::NotArmed("armed without signing key — cannot authorize swaps".into())
        })?;
        Keypair::try_from(bytes.as_slice()).map_err(|e| {
            ExecError::NotArmed(format!("signing key unusable, refusing to trade: {e}"))
        })
    }

    /// Blockhash manager: fresh-from-RPC when nothing is cached or the
    /// cached entry outlived its TTL, otherwise the cached value. Every
    /// bundle still gets a RECENT blockhash; back-to-back bundles inside one
    /// TTL window share it instead of hammering the RPC.
    async fn fresh_blockhash(&mut self) -> Result<(String, u64), ExecError> {
        if let Some((hash, height, at)) = &self.cached_blockhash {
            if at.elapsed() < Duration::from_secs(self.blockhash_ttl_secs) {
                return Ok((hash.clone(), *height));
            }
        }
        let (hash, height) = self.rpc.fetch_recent_blockhash().await?;
        self.cached_blockhash = Some((hash.clone(), height, Instant::now()));
        Ok((hash, height))
    }

    /// Post-landing reconciliation (the pre-mainnet gap this closes): after a
    /// bundle LANDS, the wallet must show what the fill claims. Buys require
    /// balance(mint) to cover the filled qty within dust; sells (which always
    /// flatten) require balance(mint) to be dust-or-empty.
    /// A mismatch is TRANSPORT (unknown state — the chain moved, our book may
    /// be wrong): the engine alerts, never records the fill as fact, and
    /// never blind-resubmits. Skipped when nothing was sent (simulate-only).
    async fn reconcile_buy_fill(&self, mint: &str, qty: Decimal) -> Result<(), ExecError> {
        let actual = self
            .rpc
            .fetch_token_balance(&self.operator_pubkey, mint)
            .await?;
        if actual + self.reconcile_dust_tokens < qty {
            return Err(ExecError::Transport(format!(
                "reconcile mismatch after landed buy of {mint}: wallet shows {actual}, fill claims {qty} (dust tol {})",
                self.reconcile_dust_tokens
            )));
        }
        Ok(())
    }

    async fn reconcile_sell_fill(&self, mint: &str) -> Result<(), ExecError> {
        let actual = self
            .rpc
            .fetch_token_balance(&self.operator_pubkey, mint)
            .await?;
        if actual > self.reconcile_dust_tokens {
            return Err(ExecError::Transport(format!(
                "tokens remain after landed sell of {mint}: wallet shows {actual} (dust tol {})",
                self.reconcile_dust_tokens
            )));
        }
        Ok(())
    }

    /// Shared tail of both directions: sign the assembled swap, build + sign
    /// the tier's tip transfer on the SAME blockhash, then either report both
    /// as simulated (default) or submit the [swap, tip] bundle + reconcile
    /// landing. Returns the landed (or empty-for-simulated) signatures.
    async fn sign_and_maybe_send(
        &mut self,
        swap_tx: &SwapTx,
        tier: crate::exec::TipTier,
    ) -> Result<Vec<String>, ExecError> {
        let signer = self.signer()?;
        let (blockhash, _height) = self.fresh_blockhash().await?;
        let signed_b64 = sign_versioned_tx_b64(&swap_tx.tx_base64, &signer, &blockhash)?;
        // One urgency price, two expressions: the swap's priority fee (set at
        // assembly) + this standalone tip transfer. No tip account on file =
        // REFUSED (fail-closed: a tipless bundle defeats the urgency tiers,
        // and silently sending one would lie about the order's bid).
        let tip_lamports = self.jito.tip_for(tier);
        let tip_b64 = if tip_lamports == 0 {
            None
        } else {
            let n = self.next_id();
            let accounts = self
                .jito
                .tip_accounts
                .iter()
                .map(|s| parse_tip_account(s))
                .collect::<Result<Vec<_>, _>>()?;
            let dest = pick_tip_account(&accounts, n).ok_or_else(|| {
                ExecError::Rejected(
                    "no Jito tip account configured — set HFM_JITO_TIP_ACCOUNT (real sends are fail-closed without the tip transfer)".into(),
                )
            })?;
            Some(build_tip_tx_b64(&signer, &dest, tip_lamports, &blockhash)?)
        };
        let txs = assemble_bundle(signed_b64, tip_b64);
        if self.simulate_only {
            // M4 default: the FULL pipeline ran (quote → assemble → fresh
            // blockhash → real signatures on swap AND tip) but nothing was
            // submitted. Loud on purpose — a simulated fill must never be
            // mistaken for a fill.
            tracing::warn!(
                "SIMULATE-ONLY: swap+tip assembled+signed, NOT submitted (set HFM_SIMULATE_ONLY=false to send)"
            );
            return Ok(Vec::new());
        }
        let id = self.next_id();
        let bundle_uuid = self.jito.send_bundle(id, &txs).await?;
        tracing::info!(bundle = %bundle_uuid, "bundle accepted, reconciling landing");
        let sigs = self
            .jito
            .poll_bundle_landing(&bundle_uuid, self.bundle_timeout_secs)
            .await?;
        tracing::info!(
            bundle = %bundle_uuid,
            signatures = ?sigs.first(),
            "bundle landed"
        );
        Ok(sigs)
    }

    /// Jito tip expressed in USD at the SOL/USD reference — the only live
    /// cost we can account exactly (quoted route fees stay inside the
    /// quote). Zero in simulate-only (nothing was sent). The tier is the one
    /// actually used for the order, so flip exits account their higher bid.
    fn tip_fee_usd(&self, tier: crate::exec::TipTier, sent: bool) -> Decimal {
        if sent {
            lamports_to_usd(self.jito.tip_for(tier), self.sol_usd)
        } else {
            Decimal::ZERO
        }
    }

    /// M4 buy pipeline: USD budget → lamports → quote → preflight →
    /// swap assembly → sign → (maybe) bundle → Fill from quote math.
    async fn execute_live_buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        _now: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        let lamports = usd_to_lamports_floor(budget_usd, self.sol_usd)?;
        let bps = slippage_pct_to_bps(self.max_slippage_pct)?;
        let quote = self.jupiter.quote(SOL_MINT, mint, lamports, bps).await?;
        if quote.input_mint != SOL_MINT || quote.output_mint != mint {
            return Err(ExecError::Rejected(format!(
                "jupiter quote pair mismatch (want SOL→{mint}) — refusing"
            )));
        }
        if quote.out_amount_raw == 0 {
            return Err(ExecError::Rejected(format!(
                "jupiter quote for {mint} returns zero output — refusing"
            )));
        }
        quote_within_budget(&quote, self.max_slippage_pct)?;
        let out_decimals = self.rpc.fetch_token_decimals(mint).await?;
        // Entries bid the patient tier — one urgency price, two expressions
        // (swap priority fee at assembly + tip transfer in the bundle).
        let priority_fee = self.jito.tips.entry;
        let swap_tx = self
            .jupiter
            .swap(&quote, &self.operator_pubkey, priority_fee)
            .await?;
        let sigs = self
            .sign_and_maybe_send(&swap_tx, crate::exec::TipTier::Entry)
            .await?;
        let sent = !self.simulate_only;
        debug_assert!(sigs.is_empty() || sent);
        let qty = raw_to_decimal(quote.out_amount_raw, out_decimals);
        if qty <= Decimal::ZERO {
            return Err(ExecError::Rejected(format!(
                "quote output converts to zero tokens at {out_decimals} decimals"
            )));
        }
        if sent {
            self.reconcile_buy_fill(mint, qty).await?;
        }
        Ok(Fill {
            order_id: order_id.to_string(),
            mint: mint.to_string(),
            side: Side::Buy,
            qty,
            price_usd: budget_usd / qty,
            notional_usd: budget_usd,
            fee_usd: self.tip_fee_usd(crate::exec::TipTier::Entry, sent),
            ts: Utc::now(),
        })
    }

    /// M4 sell pipeline: token qty → raw units → quote → preflight →
    /// swap assembly → sign → (maybe) bundle → Fill from quote proceeds.
    /// `tier` (from the position's hold mode) sets the swap priority fee +
    /// the accounted Jito tip.
    async fn execute_live_sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        _now: DateTime<Utc>,
        order_id: &str,
        tier: crate::exec::TipTier,
    ) -> Result<Fill, ExecError> {
        let in_decimals = self.rpc.fetch_token_decimals(mint).await?;
        let in_raw = decimal_to_raw_floor(qty, in_decimals)?;
        let bps = slippage_pct_to_bps(self.max_slippage_pct)?;
        let quote = self.jupiter.quote(mint, SOL_MINT, in_raw, bps).await?;
        if quote.input_mint != mint || quote.output_mint != SOL_MINT {
            return Err(ExecError::Rejected(format!(
                "jupiter quote pair mismatch (want {mint}→SOL) — refusing"
            )));
        }
        if quote.out_amount_raw == 0 {
            return Err(ExecError::Rejected(format!(
                "jupiter quote for {mint} returns zero SOL — refusing"
            )));
        }
        quote_within_budget(&quote, self.max_slippage_pct)?;
        let priority_fee = self.jito.tip_for(tier);
        let swap_tx = self
            .jupiter
            .swap(&quote, &self.operator_pubkey, priority_fee)
            .await?;
        let sigs = self.sign_and_maybe_send(&swap_tx, tier).await?;
        let sent = !self.simulate_only;
        debug_assert!(sigs.is_empty() || sent);
        if sent {
            self.reconcile_sell_fill(mint).await?;
        }
        let proceeds_usd = lamports_to_usd(quote.out_amount_raw, self.sol_usd);
        Ok(Fill {
            order_id: order_id.to_string(),
            mint: mint.to_string(),
            side: Side::Sell,
            qty,
            price_usd: proceeds_usd / qty,
            notional_usd: proceeds_usd,
            fee_usd: self.tip_fee_usd(tier, sent),
            ts: Utc::now(),
        })
    }
}

#[async_trait]
impl Executor for LiveExecutor {
    async fn buy(
        &mut self,
        mint: &str,
        budget_usd: Decimal,
        _price_usd: Decimal,
        _liquidity_usd: Decimal,
        now: DateTime<Utc>,
        deadline: DateTime<Utc>,
        order_id: &str,
    ) -> Result<Fill, ExecError> {
        if !self.armed {
            return Err(ExecError::NotArmed(format!(
                "live executor unarmed (operator {}); paper runs must use PaperExecutor",
                self.operator_pubkey
            )));
        }
        // D11 is enforced here, not just documented: a live executor must
        // refuse slices past the funnel deadline.
        if now > deadline {
            return Err(ExecError::Rejected(format!(
                "funnel_window_expired: slice for {mint} (order {order_id}) past deadline"
            )));
        }
        self.execute_live_buy(mint, budget_usd, now, order_id).await
    }

    async fn sell(
        &mut self,
        mint: &str,
        qty: Decimal,
        _price_usd: Decimal,
        _liquidity_usd: Decimal,
        now: DateTime<Utc>,
        order_id: &str,
        tier: crate::exec::TipTier,
    ) -> Result<Fill, ExecError> {
        if !self.armed {
            return Err(ExecError::NotArmed(format!(
                "live executor unarmed (operator {}); paper runs must use PaperExecutor",
                self.operator_pubkey
            )));
        }
        self.execute_live_sell(mint, qty, now, order_id, tier).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const QUOTE_FIXTURE: &str = r#"{
        "inputMint": "So11111111111111111111111111111111111111112",
        "inAmount": "100000000",
        "outputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "outAmount": "17057460",
        "otherAmountThreshold": "16886885",
        "swapMode": "ExactIn",
        "slippageBps": 100,
        "platformFee": null,
        "priceImpactPct": "0.0001",
        "routePlan": [],
        "contextSlot": 324307186,
        "timeTaken": 0.012
    }"#;

    #[test]
    fn quote_url_pins_exactin_jito_compatible_params() {
        let url = JupiterClient::quote_url(
            "https://api.jup.ag/swap/v1/",
            "So11111111111111111111111111111111111111112",
            "MINT",
            1_000_000_000,
            100,
        );
        assert!(url.starts_with("https://api.jup.ag/swap/v1/quote?"));
        for param in [
            "inputMint=So11111111111111111111111111111111111111112",
            "outputMint=MINT",
            "amount=1000000000",
            "slippageBps=100",
            "swapMode=ExactIn",
            "forJitoBundle=true",
            "onlyDirectRoutes=true",
        ] {
            assert!(url.contains(param), "missing {param}: {url}");
        }
    }

    #[test]
    fn quote_parser_reads_realistic_response() {
        let q = parse_quote_response(QUOTE_FIXTURE).unwrap();
        assert_eq!(q.in_amount_raw, 100_000_000);
        assert_eq!(q.out_amount_raw, 17_057_460);
        assert_eq!(q.other_amount_threshold_raw, 16_886_885);
        assert_eq!(q.swap_mode, "ExactIn");
        assert_eq!(q.slippage_bps, 100);
        assert_eq!(q.price_impact_pct, dec!(0.0001));
    }

    #[test]
    fn quote_parser_refuses_garbage() {
        assert!(parse_quote_response("not json").is_err());
        assert!(parse_quote_response(r#"{"inputMint":"X"}"#).is_err());
        // outAmount must be a u64 — floats/decimals are a hard refuse.
        assert!(parse_quote_response(
            r#"{"inputMint":"A","outputMint":"B","inAmount":"1","outAmount":"1.5","otherAmountThreshold":"1","swapMode":"ExactIn","slippageBps":50,"priceImpactPct":"0"}"#
        )
        .is_err());
    }

    #[test]
    fn preflight_rejects_over_budget_quotes() {
        let mut q = parse_quote_response(QUOTE_FIXTURE).unwrap();
        assert!(quote_within_budget(&q, dec!(10)).is_ok());
        q.price_impact_pct = dec!(10.01);
        assert!(quote_within_budget(&q, dec!(10)).is_err());
    }

    #[test]
    fn jito_bundle_request_shape() {
        let txs = vec!["dGVzdA==".to_string(), "c2ln".to_string()];
        let req = JitoClient::build_send_bundle_request(7, &txs);
        assert_eq!(req["jsonrpc"], "2.0");
        assert_eq!(req["id"], 7);
        assert_eq!(req["method"], "sendBundle");
        assert_eq!(req["params"][0][0], "dGVzdA==");
        assert_eq!(req["params"][0][1], "c2ln");
        assert_eq!(req["params"][1]["encoding"], "base64");
    }

    #[test]
    fn token_bucket_bursts_then_blocks() {
        let mut b = TokenBucket::new(2, 1_000); // fast refill, but sync takes pass no time
        assert!(b.try_take(1));
        assert!(b.try_take(1));
        assert!(
            !b.try_take(1),
            "burst exhausted: third immediate take fails"
        );
        assert!(b.try_take(0), "zero take always succeeds");
    }

    #[test]
    fn live_executor_refuses_to_arm_in_paper() {
        let cfg = Config::paper_defaults();
        assert!(LiveExecutor::armed(&cfg, "SomePubkey").is_err());
        assert!(LiveExecutor::armed(&cfg, "").is_err());
        let unarmed = LiveExecutor::unarmed_for_paper(&cfg, "SomePubkey");
        assert!(!unarmed.is_armed());
    }

    #[tokio::test]
    async fn unarmed_executor_rejects_without_network() {
        let cfg = Config::paper_defaults();
        let mut ex = LiveExecutor::unarmed_for_paper(&cfg, "SomePubkey");
        let now = Utc::now();
        let err = ex
            .buy("M", dec!(100), dec!(1), dec!(8000), now, now, "o1")
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::NotArmed(_)));
        let err = ex
            .sell(
                "M",
                dec!(1),
                dec!(1),
                dec!(8000),
                now,
                "o2",
                crate::exec::TipTier::FlipExit,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::NotArmed(_)));
    }

    #[tokio::test]
    async fn armed_executor_enforces_funnel_deadline() {
        let mut cfg = Config::paper_defaults();
        cfg.mode = Mode::Live;
        let mut ex = LiveExecutor::armed(&cfg, "SomePubkey").unwrap();
        assert!(ex.is_armed());
        let now = Utc::now();
        // Slice past the deadline is refused BEFORE any network/swap work.
        let err = ex
            .buy(
                "M",
                dec!(100),
                dec!(1),
                dec!(8000),
                now,
                now - chrono::Duration::seconds(1),
                "late",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::Rejected(_)));
        // In-window slice reaches the pipeline — but paper defaults carry no
        // SOL/USD reference, so it is refused BEFORE any network/swap work
        // (still no funds move, still no I/O: conversion is the first step).
        let err = ex
            .buy(
                "M",
                dec!(100),
                dec!(1),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "early",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::Rejected(_)));
    }

    // ---- M4 pure unit tests: conversions, builders, parsers ----

    #[test]
    fn usd_to_lamports_floors_never_rounds_up() {
        // Exact: 103.77 USD @ 103.77 = exactly 1 SOL.
        assert_eq!(
            usd_to_lamports_floor(dec!(103.77), dec!(103.77)).unwrap(),
            1_000_000_000
        );
        // Floor: 100 USD @ 103.77 → 0.9636… SOL, never a lamport more.
        let lamports = usd_to_lamports_floor(dec!(100), dec!(103.77)).unwrap();
        assert!(lamports_to_usd(lamports, dec!(103.77)) <= dec!(100));
        assert!(lamports_to_usd(lamports + 1, dec!(103.77)) > dec!(100));
        // Fail-closed inputs.
        assert!(usd_to_lamports_floor(dec!(100), dec!(0)).is_err());
        assert!(usd_to_lamports_floor(dec!(0), dec!(100)).is_err());
        assert!(usd_to_lamports_floor(dec!(-5), dec!(100)).is_err());
        assert!(usd_to_lamports_floor(dec!(0.000000000001), dec!(1000000)).is_err());
    }

    #[test]
    fn raw_unit_conversions_roundtrip() {
        assert_eq!(raw_to_decimal(5_000_000, 6), dec!(5));
        assert_eq!(raw_to_decimal(1, 9), dec!(0.000000001));
        assert_eq!(decimal_to_raw_floor(dec!(5), 6).unwrap(), 5_000_000);
        // Floor: 5.9999999 tokens at 6dp → 5_999_999 raw, never 6M.
        assert_eq!(
            decimal_to_raw_floor(dec!(5.9999999), 6).unwrap(),
            5_999_999
        );
        assert!(decimal_to_raw_floor(dec!(0), 6).is_err());
        assert!(decimal_to_raw_floor(dec!(0.0000001), 6).is_err());
    }

    #[test]
    fn slippage_bps_floors() {
        assert_eq!(slippage_pct_to_bps(dec!(10)).unwrap(), 1000);
        assert_eq!(slippage_pct_to_bps(dec!(0.5)).unwrap(), 50);
        // 10.009% → 1000bps, never 1001 (never MORE slippage than configured).
        assert_eq!(slippage_pct_to_bps(dec!(10.009)).unwrap(), 1000);
        assert!(slippage_pct_to_bps(dec!(-1)).is_err());
    }

    #[test]
    fn swap_request_carries_quote_verbatim_with_operator_payer() {
        let quote = parse_quote_response(QUOTE_FIXTURE).unwrap();
        let req = build_swap_request(&quote.raw, "OperatorPubkey111", 5000);
        assert_eq!(req["quoteResponse"]["outAmount"], "17057460");
        assert_eq!(req["userPublicKey"], "OperatorPubkey111");
        assert_eq!(req["wrapAndUnwrapSol"], true);
        assert_eq!(req["dynamicComputeUnitLimit"], true);
        assert_eq!(req["prioritizationFeeLamports"], 5000);
    }

    #[test]
    fn tip_schedule_maps_tiers() {
        let client = JitoClient {
            base_url: "http://example.invalid".into(),
            tips: TipSchedule {
                entry: 1_000,
                flip: 5_000,
                conviction: 2_000,
            },
            tip_accounts: Vec::new(),
            http: reqwest::Client::new(),
        };
        use crate::exec::TipTier;
        assert_eq!(client.tip_for(TipTier::Entry), 1_000);
        assert_eq!(client.tip_for(TipTier::FlipExit), 5_000);
        assert_eq!(client.tip_for(TipTier::ConvictionExit), 2_000);
    }

    #[test]
    fn tip_account_parser_accepts_valid_rejects_garbage() {
        // System program id + SOL mint are both well-formed pubkeys.
        assert!(parse_tip_account(SYSTEM_PROGRAM_ID_STR).is_ok());
        assert!(parse_tip_account(SOL_MINT).is_ok());
        assert!(parse_tip_account(&format!("  {SOL_MINT}  ")).is_ok());
        for bad in [
            "",
            "   ",
            "not-a-key",
            "1111",
            "00000000000000000000000000000000",
        ] {
            assert!(
                parse_tip_account(bad).is_err(),
                "must refuse tip account '{bad}'"
            );
        }
    }

    #[test]
    fn tip_pick_round_robins_and_empty_is_none() {
        let a = parse_tip_account(SYSTEM_PROGRAM_ID_STR).unwrap();
        let b = parse_tip_account(SOL_MINT).unwrap();
        let c = Keypair::new().pubkey();
        let accounts = vec![a, b, c];
        let picked: Vec<Pubkey> = (0..5)
            .map(|n| pick_tip_account(&accounts, n).unwrap())
            .collect();
        assert_eq!(picked, vec![a, b, c, a, b]);
        assert!(pick_tip_account(&[], 0).is_none());
        assert!(pick_tip_account(&[], 999).is_none());
    }

    #[test]
    fn tip_transfer_ix_layout_is_system_transfer() {
        let from = Keypair::new().pubkey();
        let to = parse_tip_account(SOL_MINT).unwrap();
        let lamports = 123_456_789u64;
        let ix = build_tip_transfer_ix(&from, &to, lamports).unwrap();
        // Program: the system program, re-derived from the const.
        assert_eq!(
            ix.program_id,
            Pubkey::from_str(SYSTEM_PROGRAM_ID_STR).unwrap()
        );
        // Accounts: from = signer + writable, to = writable only.
        assert_eq!(ix.accounts.len(), 2);
        assert_eq!(ix.accounts[0].pubkey, from);
        assert!(ix.accounts[0].is_signer && ix.accounts[0].is_writable);
        assert_eq!(ix.accounts[1].pubkey, to);
        assert!(!ix.accounts[1].is_signer && ix.accounts[1].is_writable);
        // Data: u32 LE 2, then u64 LE lamports — decoded back, not eyeballed.
        assert_eq!(ix.data.len(), 12);
        assert_eq!(u32::from_le_bytes(ix.data[0..4].try_into().unwrap()), 2);
        assert_eq!(
            u64::from_le_bytes(ix.data[4..12].try_into().unwrap()),
            lamports
        );
    }

    #[test]
    fn tip_tx_builds_signed_transfer_with_fresh_blockhash() {
        use base64::Engine as _;
        let kp = Keypair::new();
        let tip = parse_tip_account(SYSTEM_PROGRAM_ID_STR).unwrap();
        let blockhash = "11111111111111111111111111111111";
        let lamports = 2_000_000u64;
        let tx_b64 = build_tip_tx_b64(&kp, &tip, lamports, blockhash).unwrap();
        let bytes = base64::prelude::BASE64_STANDARD.decode(&tx_b64).unwrap();
        let tx: VersionedTransaction = bincode::deserialize(&bytes).unwrap();
        // Operator pays, exactly one signature, and it verifies.
        let VersionedMessage::Legacy(msg) = &tx.message else {
            panic!("tip tx must be a legacy message");
        };
        assert_eq!(msg.instructions.len(), 1);
        assert_eq!(msg.account_keys[0], kp.pubkey());
        assert_eq!(tx.signatures.len(), 1);
        assert!(tx.signatures[0].verify(&kp.pubkey().to_bytes(), &tx.message.serialize()));
        // The compiled instruction resolves to system-transfer(from → tip).
        let compiled = &msg.instructions[0];
        assert_eq!(
            msg.account_keys[compiled.program_id_index as usize],
            Pubkey::from_str(SYSTEM_PROGRAM_ID_STR).unwrap()
        );
        let from_key = msg.account_keys[compiled.accounts[0] as usize];
        let to_key = msg.account_keys[compiled.accounts[1] as usize];
        assert_eq!(from_key, kp.pubkey());
        assert_eq!(to_key, tip);
        assert_eq!(compiled.data.len(), 12);
        assert_eq!(
            u32::from_le_bytes(compiled.data[0..4].try_into().unwrap()),
            2
        );
        assert_eq!(
            u64::from_le_bytes(compiled.data[4..12].try_into().unwrap()),
            lamports
        );
        // Zero lamports and garbage blockhash are hard refuses.
        assert!(build_tip_tx_b64(&kp, &tip, 0, blockhash).is_err());
        assert!(build_tip_tx_b64(&kp, &tip, lamports, "nope").is_err());
    }

    #[test]
    fn assemble_bundle_orders_swap_first_tip_second() {
        assert_eq!(
            assemble_bundle("swap".into(), Some("tip".into())),
            vec!["swap".to_string(), "tip".to_string()]
        );
        assert_eq!(
            assemble_bundle("swap".into(), None),
            vec!["swap".to_string()]
        );
    }

    #[test]
    fn swap_response_parser() {
        let q = parse_swap_response(
            r#"{"swapTransaction":"dGVzdA==","lastValidBlockHeight":424242,"other":"ignored"}"#,
        )
        .unwrap();
        assert_eq!(q.tx_base64, "dGVzdA==");
        assert_eq!(q.last_valid_block_height, 424242);
        assert!(parse_swap_response(r#"{"lastValidBlockHeight":1}"#).is_err());
        assert!(parse_swap_response(r#"{"swapTransaction":"","lastValidBlockHeight":1}"#).is_err());
        assert!(parse_swap_response("nope").is_err());
    }

    #[test]
    fn blockhash_parser_accepts_only_decodable_hashes() {
        let (bh, h) = parse_blockhash_response(
            r#"{"result":{"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":999}}}"#,
        )
        .unwrap();
        assert_eq!(bh, "11111111111111111111111111111111");
        assert_eq!(h, 999);
        assert!(parse_blockhash_response(
            r#"{"result":{"value":{"blockhash":"not-a-hash!","lastValidBlockHeight":1}}}"#
        )
        .is_err());
        assert!(parse_blockhash_response(r#"{"result":{}}"#).is_err());
    }

    #[test]
    fn token_supply_parser_reads_decimals() {
        assert_eq!(
            parse_token_supply_decimals(
                r#"{"result":{"value":{"decimals":6,"supply":"1000","uiAmount":0.001}}}"#
            )
            .unwrap(),
            6
        );
        assert!(parse_token_supply_decimals(r#"{"result":{"value":{}}}"#).is_err());
    }

    #[test]
    fn bundle_statuses_builder_shape() {
        let uuids = vec!["uuid-1".to_string()];
        let req = JitoClient::build_get_bundle_statuses_request(3, &uuids);
        assert_eq!(req["method"], "getBundleStatuses");
        assert_eq!(req["params"][0][0], "uuid-1");
        assert_eq!(req["jsonrpc"], "2.0");
    }

    #[test]
    fn bundle_status_parser_only_explicit_landing_counts() {
        // Finalized + Ok → landed with signatures.
        let landed = parse_bundle_status(
            r#"{"result":{"value":[{"bundle_id":"u","transactions":["sigA","sigB"],"slot":1,"confirmation_status":"finalized","err":{"Ok":null}}]}}"#,
        )
        .unwrap();
        assert_eq!(
            landed,
            BundleStatus::Landed {
                signatures: vec!["sigA".to_string(), "sigB".to_string()]
            }
        );
        // Explicit on-chain error → failed.
        let failed = parse_bundle_status(
            r#"{"result":{"value":[{"bundle_id":"u","confirmation_status":"finalized","err":{"Err":{"InstructionError":[0,"Custom"]}}}]}}"#,
        )
        .unwrap();
        assert!(matches!(failed, BundleStatus::Failed { .. }));
        // Processed-but-not-confirmed → keep waiting, never success.
        let pending = parse_bundle_status(
            r#"{"result":{"value":[{"bundle_id":"u","confirmation_status":"processed","err":{"Ok":null}}]}}"#,
        )
        .unwrap();
        assert_eq!(pending, BundleStatus::Pending);
        // Unknown/empty → pending, never success.
        assert_eq!(
            parse_bundle_status(r#"{"result":{"value":[]}}"#).unwrap(),
            BundleStatus::Pending
        );
        assert_eq!(
            parse_bundle_status(r#"{"result":{}}"#).unwrap(),
            BundleStatus::Pending
        );
    }

    // ---- signing tests: real keypair, real versioned-tx bytes ----

    /// Build Jupiter-shaped unsigned bytes: a v0 tx paying from the keypair
    /// with a placeholder signature, bincode-serialized + base64.
    fn unsigned_swap_fixture_b64(kp: &Keypair) -> String {
        use base64::Engine as _;
        let msg = solana_message::v0::Message::try_compile(
            &kp.pubkey(),
            &[],
            &[],
            solana_hash::Hash::default(),
        )
        .unwrap();
        let tx = VersionedTransaction {
            signatures: vec![solana_signature::Signature::default()],
            message: VersionedMessage::V0(msg),
        };
        base64::prelude::BASE64_STANDARD.encode(bincode::serialize(&tx).unwrap())
    }

    #[test]
    fn sign_roundtrip_replaces_blockhash_and_verifies() {
        use base64::Engine as _;
        let kp = Keypair::new();
        let unsigned = unsigned_swap_fixture_b64(&kp);
        let signed_b64 =
            sign_versioned_tx_b64(&unsigned, &kp, "11111111111111111111111111111111").unwrap();
        assert_ne!(signed_b64, unsigned, "signing must change the bytes");
        let bytes = base64::prelude::BASE64_STANDARD
            .decode(&signed_b64)
            .unwrap();
        let tx: VersionedTransaction = bincode::deserialize(&bytes).unwrap();
        // Fresh blockhash stamped, payer preserved.
        let expected = solana_hash::Hash::from_str("11111111111111111111111111111111").unwrap();
        assert_eq!(tx.message.recent_blockhash(), &expected);
        assert_eq!(&tx.message.static_account_keys()[0], &kp.pubkey());
        // Exactly one real signature, and it cryptographically verifies.
        assert_eq!(tx.signatures.len(), 1);
        assert_ne!(tx.signatures[0], solana_signature::Signature::default());
        assert!(tx.signatures[0].verify(&kp.pubkey().to_bytes(), &tx.message.serialize()));
    }

    #[test]
    fn sign_refuses_foreign_payer_and_garbage() {
        let kp = Keypair::new();
        let stranger = Keypair::new();
        let foreign = unsigned_swap_fixture_b64(&stranger);
        let err = sign_versioned_tx_b64(&foreign, &kp, "11111111111111111111111111111111")
            .unwrap_err();
        assert!(matches!(err, ExecError::Rejected(_)));
        assert!(sign_versioned_tx_b64("!!!not-base64!!!", &kp, "11111111111111111111111111111111")
            .is_err());
        assert!(sign_versioned_tx_b64("dGVzdA==", &kp, "11111111111111111111111111111111").is_err());
    }

    // ---- M4 mock-upstream e2e: full pipeline against local fakes ----
    //
    // One tiny HTTP mock plays Jupiter (`/quote`, `/swap`), Solana RPC
    // (`getLatestBlockhash`, `getTokenSupply`) and Jito (`sendBundle`,
    // `getBundleStatuses`). The executor under test cannot tell them from
    // the real upstreams — same URLs, same JSON shapes — so a passing run
    // proves the whole quote → assemble → sign → send → reconcile path.

    const E2E_MINT: &str = "MockMint11111111111111111111111111111111";

    const E2E_BUY_QUOTE: &str = r#"{
        "inputMint": "So11111111111111111111111111111111111111112",
        "inAmount": "1000000000",
        "outputMint": "MockMint11111111111111111111111111111111",
        "outAmount": "5000000",
        "otherAmountThreshold": "4950000",
        "swapMode": "ExactIn",
        "slippageBps": 1000,
        "priceImpactPct": "0.5"
    }"#;

    const E2E_SELL_QUOTE: &str = r#"{
        "inputMint": "MockMint11111111111111111111111111111111",
        "inAmount": "5000000",
        "outputMint": "So11111111111111111111111111111111111111112",
        "outAmount": "500000000",
        "otherAmountThreshold": "495000000",
        "swapMode": "ExactIn",
        "slippageBps": 1000,
        "priceImpactPct": "0.5"
    }"#;

    struct MockUpstream {
        swap_b64: String,
        bundle_hits: std::sync::atomic::AtomicU64,
        /// Tx count of the most recent sendBundle body (proves bundle shape:
        /// swap-only vs swap+tip).
        last_bundle_txs: std::sync::atomic::AtomicU64,
        /// getLatestBlockhash calls served (proves WHICH endpoint answered).
        rpc_hits: std::sync::atomic::AtomicU64,
        /// 0 = bundles land immediately, 1 = always pending (timeout test).
        mode: std::sync::atomic::AtomicU8,
        /// Operator token balance served to getTokenAccountsByOwner, in raw
        /// micro-units @6dp (tests set this to match — or break — the fill).
        token_balance_raw: std::sync::atomic::AtomicU64,
    }

    fn route_mock(request_line: &str, body: &str, state: &MockUpstream) -> String {
        use std::sync::atomic::Ordering;
        if request_line.starts_with("GET /quote") {
            // Direction-correct quotes: the executor refuses pair mismatches.
            if request_line.contains("inputMint=So111") {
                return E2E_BUY_QUOTE.to_string();
            }
            return E2E_SELL_QUOTE.to_string();
        }
        if request_line.starts_with("POST /swap") {
            return format!(
                r#"{{"swapTransaction":"{}","lastValidBlockHeight":424242}}"#,
                state.swap_b64
            );
        }
        if request_line.starts_with("POST /api/v1/bundles") {
            if body.contains("sendBundle") {
                state.bundle_hits.fetch_add(1, Ordering::SeqCst);
                // Bundle shape proof: params[0] is the tx array.
                let n = serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|v| v.get("params").cloned())
                    .and_then(|p| p.get(0).cloned())
                    .and_then(|t| t.as_array().cloned())
                    .map(|a| a.len() as u64)
                    .unwrap_or(u64::MAX);
                state.last_bundle_txs.store(n, Ordering::SeqCst);
                return r#"{"jsonrpc":"2.0","id":1,"result":"mock-bundle-uuid-9"}"#.to_string();
            }
            if state.mode.load(Ordering::SeqCst) == 0 {
                return r#"{"jsonrpc":"2.0","id":1,"result":{"value":[{"bundle_id":"mock-bundle-uuid-9","transactions":["sigLanded111"],"slot":7,"confirmation_status":"finalized","err":{"Ok":null}}]}}"#.to_string();
            }
            return r#"{"jsonrpc":"2.0","id":1,"result":{"value":[]}}"#.to_string();
        }
        if body.contains("getLatestBlockhash") {
            state.rpc_hits.fetch_add(1, Ordering::SeqCst);
            return r#"{"jsonrpc":"2.0","id":1,"result":{"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":999}}}"#.to_string();
        }
        if body.contains("getTokenSupply") {
            return r#"{"jsonrpc":"2.0","id":1,"result":{"value":{"decimals":6,"supply":"1000000000"}}}"#.to_string();
        }
        if body.contains("requestAirdrop") {
            return r#"{"jsonrpc":"2.0","id":1,"result":"mock-airdrop-sig"}"#.to_string();
        }
        if body.contains("\"getBalance\"") {
            return r#"{"jsonrpc":"2.0","id":1,"result":{"value":1000000000}}"#.to_string();
        }
        if body.contains("sendTransaction") {
            return r#"{"jsonrpc":"2.0","id":1,"result":"mock-tx-sig"}"#.to_string();
        }
        if body.contains("getSignatureStatuses") {
            return r#"{"jsonrpc":"2.0","id":1,"result":{"value":[{"slot":7,"confirmations":32,"confirmationStatus":"finalized","err":{"Ok":null}}]}}"#.to_string();
        }
        if body.contains("getTokenAccountsByOwner") {
            let raw = state.token_balance_raw.load(Ordering::SeqCst);
            let ui = format!("{}.{:06}", raw / 1_000_000, raw % 1_000_000);
            return format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"value":[{{"account":{{"data":{{"parsed":{{"info":{{"tokenAmount":{{"amount":"{raw}","decimals":6,"uiAmountString":"{ui}"}}}}}}}}}}}}]}}}}"#
            );
        }
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"mock: unknown route"}}"#.to_string()
    }

    async fn mock_handle(
        mut sock: tokio::net::TcpStream,
        state: std::sync::Arc<MockUpstream>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut chunk = vec![0u8; 8192];
        let mut data: Vec<u8> = Vec::new();
        let head_end = loop {
            let n = sock.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                return;
            }
            data.extend_from_slice(&chunk[..n]);
            if let Some(p) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                break p + 4;
            }
            if data.len() > 65536 {
                return;
            }
        };
        let head = String::from_utf8_lossy(&data[..head_end]).to_string();
        let content_len: usize = head
            .lines()
            .find_map(|l| {
                l.strip_prefix("Content-Length:")
                    .or_else(|| l.strip_prefix("content-length:"))
            })
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        while data.len() < head_end + content_len {
            let n = sock.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            data.extend_from_slice(&chunk[..n]);
        }
        let body = String::from_utf8_lossy(&data[head_end..]).to_string();
        let request_line = head.lines().next().unwrap_or("").to_string();
        let resp_body = route_mock(&request_line, &body, &state);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            resp_body.len(),
            resp_body
        );
        let _ = sock.write_all(resp.as_bytes()).await;
    }

    async fn spawn_mock(state: std::sync::Arc<MockUpstream>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let st = state.clone();
                tokio::spawn(async move { mock_handle(sock, st).await });
            }
        });
        base
    }

    fn e2e_config(base: &str, simulate_only: bool) -> Config {
        let mut cfg = Config::paper_defaults();
        cfg.mode = Mode::Live;
        cfg.sol_usd = dec!(100);
        cfg.simulate_only = simulate_only;
        cfg.max_slippage_pct = dec!(10);
        cfg.jupiter_url = base.to_string();
        cfg.jito_url = base.to_string();
        cfg.rpc_url = base.to_string();
        // The mock never moves funds — any valid pubkey exercises the tip path.
        cfg.jito_tip_accounts = vec![SOL_MINT.to_string()];
        cfg
    }

    fn e2e_loaded(kp: Keypair) -> LoadedKey {
        let pubkey_base58 = kp.pubkey().to_string();
        LoadedKey {
            keypair: kp,
            source: crate::keys::KeySource::SecretKey,
            pubkey_base58,
        }
    }

    fn e2e_mock(swap_b64: String, pending_mode: bool) -> std::sync::Arc<MockUpstream> {
        std::sync::Arc::new(MockUpstream {
            swap_b64,
            bundle_hits: std::sync::atomic::AtomicU64::new(0),
            last_bundle_txs: std::sync::atomic::AtomicU64::new(0),
            rpc_hits: std::sync::atomic::AtomicU64::new(0),
            mode: std::sync::atomic::AtomicU8::new(u8::from(pending_mode)),
            token_balance_raw: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Discard port: nothing listens, connections refuse fast. The failover
    /// tests' "dead primary".
    const DEAD_RPC: &str = "http://127.0.0.1:9";

    #[tokio::test]
    async fn rpc_failover_recovers_on_fallback_and_sticks() {
        use std::sync::atomic::Ordering;
        let state = e2e_mock(String::new(), false);
        let base = spawn_mock(state.clone()).await;
        let rpc = RpcClient::new_for_test_many(vec![DEAD_RPC.into(), base.clone()]);
        let (hash, height) = rpc
            .fetch_recent_blockhash()
            .await
            .expect("fallback must serve");
        assert_eq!(height, 999);
        assert!(!hash.is_empty());
        assert_eq!(state.rpc_hits.load(Ordering::SeqCst), 1);
        assert_eq!(rpc.active_url(), base.as_str());
        // Second call goes STRAIGHT to the known-good endpoint: no
        // dead-primary hammering, exactly one more mock hit.
        rpc.fetch_recent_blockhash().await.unwrap();
        assert_eq!(state.rpc_hits.load(Ordering::SeqCst), 2);
        assert_eq!(rpc.active_url(), base.as_str());
    }

    #[tokio::test]
    async fn rpc_healthy_primary_never_touches_fallback() {
        use std::sync::atomic::Ordering;
        let state = e2e_mock(String::new(), false);
        let base = spawn_mock(state.clone()).await;
        let rpc = RpcClient::new_for_test_many(vec![base.clone(), DEAD_RPC.into()]);
        rpc.fetch_recent_blockhash().await.unwrap();
        assert_eq!(state.rpc_hits.load(Ordering::SeqCst), 1);
        assert_eq!(rpc.active_url(), base.as_str());
    }

    #[tokio::test]
    async fn rpc_failover_all_dead_is_transport_unknown() {
        let rpc = RpcClient::new_for_test_many(vec![
            "http://127.0.0.1:9".into(),
            "http://127.0.0.1:8".into(),
        ]);
        let err = rpc
            .fetch_recent_blockhash()
            .await
            .expect_err("all-dead RPC must fail");
        // Transport (unknown), never Rejected: the caller must reconcile,
        // not assume the read never happened.
        assert!(
            matches!(err, ExecError::Transport(_)),
            "expected Transport, got {err:?}"
        );
    }

    #[tokio::test]
    async fn e2e_buy_sends_bundle_and_lands() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        // Reconciliation: the landed buy must show 5 tokens in the wallet.
        state.token_balance_raw.store(5_000_000, Ordering::SeqCst);
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, false), &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        // $100 @ $100/SOL → 1 SOL in; mock quotes 5 tokens out @ 6dp.
        let fill = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-buy-1",
            )
            .await
            .unwrap();
        assert_eq!(fill.qty, dec!(5));
        assert_eq!(fill.notional_usd, dec!(100));
        assert_eq!(fill.price_usd, dec!(20));
        // Tip accounted: 1M lamports = 0.001 SOL = $0.10 @ $100/SOL.
        assert_eq!(fill.fee_usd, dec!(0.1));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 1);
        // Bundle shape: [swap, tip-transfer] — the tip rides INSIDE the
        // bundle now, not just as a priority fee on the swap.
        assert_eq!(state.last_bundle_txs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn e2e_send_without_tip_account_is_fail_closed() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        let mut cfg = e2e_config(&base, false);
        cfg.jito_tip_accounts.clear();
        let mut ex = LiveExecutor::armed_with_signer(&cfg, &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        let err = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-no-tip",
            )
            .await
            .expect_err("tipless sends must be refused");
        assert!(
            matches!(err, ExecError::Rejected(_)),
            "expected Rejected, got {err:?}"
        );
        assert!(
            err.to_string().contains("HFM_JITO_TIP_ACCOUNT"),
            "refusal must name the fix, got: {err}"
        );
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn e2e_zero_tip_sends_swap_only() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        let mut cfg = e2e_config(&base, false);
        // Operator priced every tier at zero: no tip tx, still lands.
        cfg.jito_tip_lamports = 0;
        cfg.jito_tip_entry_lamports = 0;
        cfg.jito_tip_flip_lamports = 0;
        cfg.jito_tip_conviction_lamports = 0;
        let mut ex = LiveExecutor::armed_with_signer(&cfg, &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        // Landed swap-only bundle must still reconcile: wallet shows the 5.
        state.token_balance_raw.store(5_000_000, Ordering::SeqCst);
        let fill = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-zero-tip",
            )
            .await
            .unwrap();
        assert_eq!(fill.fee_usd, dec!(0));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.last_bundle_txs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn e2e_buy_simulate_only_sends_nothing() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, true), &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        let fill = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-sim-1",
            )
            .await
            .unwrap();
        // Same quote math, but nothing submitted and no fee accounted.
        assert_eq!(fill.qty, dec!(5));
        assert_eq!(fill.fee_usd, dec!(0));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn e2e_sell_pipeline_collects_proceeds() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, false), &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        // 5 tokens → mock quotes 0.5 SOL out → $50 @ $100/SOL.
        let fill = ex
            .sell(
                E2E_MINT,
                dec!(5),
                dec!(10),
                dec!(8000),
                now,
                "e2e-sell-1",
                crate::exec::TipTier::FlipExit,
            )
            .await
            .unwrap();
        assert_eq!(fill.qty, dec!(5));
        assert_eq!(fill.notional_usd, dec!(50));
        assert_eq!(fill.price_usd, dec!(10));
        assert_eq!(fill.fee_usd, dec!(0.1));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn e2e_tiers_price_exits_above_entries() {
        use crate::exec::TipTier;
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        // Distinct tiers: entries 1M lamports, flip exits 3M, conviction 2M.
        // @ $100/SOL → $0.10 entry fee, $0.30 flip fee.
        let mut cfg = e2e_config(&base, false);
        cfg.jupiter_qps = 10; // 3 orders × (quote + swap) must fit the bucket
        cfg.jito_tip_entry_lamports = 1_000_000;
        cfg.jito_tip_flip_lamports = 3_000_000;
        cfg.jito_tip_conviction_lamports = 2_000_000;
        let mut ex = LiveExecutor::armed_with_signer(&cfg, &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        // Buy first: wallet must show the 5 filled tokens...
        state.token_balance_raw.store(5_000_000, Ordering::SeqCst);
        let buy = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-tier-buy",
            )
            .await
            .unwrap();
        assert_eq!(buy.fee_usd, dec!(0.1));
        // ...then the exits flatten it back to dust-or-empty.
        state.token_balance_raw.store(0, Ordering::SeqCst);
        let flip = ex
            .sell(
                E2E_MINT,
                dec!(5),
                dec!(10),
                dec!(8000),
                now,
                "e2e-tier-flip",
                TipTier::FlipExit,
            )
            .await
            .unwrap();
        assert_eq!(flip.fee_usd, dec!(0.3));
        let conv = ex
            .sell(
                E2E_MINT,
                dec!(5),
                dec!(10),
                dec!(8000),
                now,
                "e2e-tier-conv",
                TipTier::ConvictionExit,
            )
            .await
            .unwrap();
        assert_eq!(conv.fee_usd, dec!(0.2));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn e2e_keyless_armed_refuses_at_signing() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        // Armed with a pubkey string only — every upstream cooperates, but a
        // pubkey alone must never authorize a swap.
        let mut ex = LiveExecutor::armed(&e2e_config(&base, false), "SomePubkey").unwrap();
        let now = Utc::now();
        let err = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-keyless-1",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::NotArmed(_)));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn e2e_pending_bundle_times_out_as_transport() {
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), true);
        let base = spawn_mock(state.clone()).await;
        let mut cfg = e2e_config(&base, false);
        cfg.bundle_timeout_secs = 2; // keep the test fast; prod default is 60
        let mut ex = LiveExecutor::armed_with_signer(&cfg, &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        // Bundle submitted but never lands → UNKNOWN state (Transport), the
        // stuck-position path reconciles later. Never success, never silent.
        let err = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-timeout-1",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::Transport(_)));
    }

    #[test]
    fn token_balance_parser_sums_exact_strings() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"value":[
            {"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"5000000","decimals":6,"uiAmountString":"5.000000"}}}}}},
            {"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"250000","decimals":6,"uiAmountString":"0.250000"}}}}}}
        ]}}"#;
        assert_eq!(parse_token_accounts_balance(body).unwrap(), dec!(5.25));
        // Empty set = flat zero, not an error.
        assert_eq!(
            parse_token_accounts_balance(r#"{"jsonrpc":"2.0","id":1,"result":{"value":[]}}"#)
                .unwrap(),
            Decimal::ZERO
        );
        // Raw amount+decimals fallback (no uiAmountString): exact, no floats.
        let raw = r#"{"jsonrpc":"2.0","id":1,"result":{"value":[
            {"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"3","decimals":9}}}}}}
        ]}}"#;
        assert_eq!(
            parse_token_accounts_balance(raw).unwrap(),
            dec!(0.000000003)
        );
        // Unreadable balances are Rejected (parse layer), never guessed zero.
        assert!(parse_token_accounts_balance("not json").is_err());
        assert!(parse_token_accounts_balance(r#"{"result":{}}"#).is_err());
        assert!(parse_token_accounts_balance(r#"{"result":{"value":[{"account":{}}]}}"#).is_err());
    }

    #[test]
    fn send_path_parsers_accept_and_refuse() {
        // String results: airdrop + sendTransaction signatures.
        assert_eq!(
            parse_string_result(
                r#"{"jsonrpc":"2.0","id":1,"result":"sigABC"}"#,
                "sendTransaction"
            )
            .unwrap(),
            "sigABC"
        );
        // Node-level refusal is Rejected (definitive), never Transport.
        let refused = parse_string_result(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"airdrop limit"}}"#,
            "airdrop",
        )
        .unwrap_err();
        assert!(matches!(refused, ExecError::Rejected(_)));
        assert!(parse_string_result(r#"{"result":{}}"#, "airdrop").is_err());
        // Balance: plain u64 lamports.
        assert_eq!(
            parse_balance_response(r#"{"jsonrpc":"2.0","id":1,"result":{"value":1000000000}}"#)
                .unwrap(),
            1_000_000_000
        );
        assert!(parse_balance_response(r#"{"result":{"value":"x"}}"#).is_err());
        // Signature statuses: only explicit confirmed/finalized lands.
        let landed = r#"{"result":{"value":[{"confirmationStatus":"finalized","err":{"Ok":null}}]}}"#;
        assert_eq!(parse_sig_status(landed).unwrap(), SigStatus::Confirmed);
        let confirmed =
            r#"{"result":{"value":[{"confirmationStatus":"confirmed","err":null}]}}"#;
        assert_eq!(parse_sig_status(confirmed).unwrap(), SigStatus::Confirmed);
        // Null entry (node hasn't seen it) and missing status: Pending.
        assert_eq!(
            parse_sig_status(r#"{"result":{"value":[null]}}"#).unwrap(),
            SigStatus::Pending
        );
        assert_eq!(
            parse_sig_status(r#"{"result":{"value":[{"slot":7}]}}"#).unwrap(),
            SigStatus::Pending
        );
        // Explicit non-Ok err: Failed with the reason attached.
        let failed = r#"{"result":{"value":[{"err":{"InstructionError":[0,"Custom"]}}]}}"#;
        assert!(matches!(
            parse_sig_status(failed).unwrap(),
            SigStatus::Failed(_)
        ));
        assert!(parse_sig_status("garbage").is_err());
    }

    #[tokio::test]
    async fn e2e_send_path_round_trips_against_mock() {
        let state = e2e_mock(String::new(), false);
        let base = spawn_mock(state.clone()).await;
        let rpc = RpcClient::new_for_test(&base);
        assert_eq!(
            rpc.request_airdrop("SomePubkey111111111111111111111111111111", 1_000_000_000)
                .await
                .unwrap(),
            "mock-airdrop-sig"
        );
        assert_eq!(
            rpc.get_balance("SomePubkey111111111111111111111111111111")
                .await
                .unwrap(),
            1_000_000_000
        );
        assert_eq!(
            rpc.send_transaction("dGVzdA==").await.unwrap(),
            "mock-tx-sig"
        );
        rpc.confirm_signature("mock-tx-sig", 5).await.unwrap();
    }

    #[tokio::test]
    async fn e2e_buy_balance_shortfall_is_transport_unknown() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        // Bundle lands, but the wallet shows 1 token against a 5-token fill.
        state.token_balance_raw.store(1_000_000, Ordering::SeqCst);
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, false), &e2e_loaded(kp)).unwrap();
        let now = Utc::now();
        let err = ex
            .buy(
                E2E_MINT,
                dec!(100),
                dec!(20),
                dec!(8000),
                now,
                now + chrono::Duration::seconds(5),
                "e2e-recon-buy-1",
            )
            .await
            .unwrap_err();
        // Landed AND refused as a fill: bundle went out (1 hit), but the
        // book records nothing — the engine alerts TransportUnknown.
        assert!(
            matches!(err, ExecError::Transport(_)),
            "expected Transport, got {err:?}"
        );
        assert!(err.to_string().contains("reconcile mismatch"));
        assert_eq!(state.bundle_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn e2e_sell_remainder_is_transport_unknown() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        // Bundle lands, but 2 tokens are still sitting in the wallet.
        state.token_balance_raw.store(2_000_000, Ordering::SeqCst);
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, false), &e2e_loaded(kp)).unwrap();
        let err = ex
            .sell(
                E2E_MINT,
                dec!(5),
                dec!(10),
                dec!(8000),
                Utc::now(),
                "e2e-recon-sell-1",
                crate::exec::TipTier::FlipExit,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExecError::Transport(_)),
            "expected Transport, got {err:?}"
        );
        assert!(err.to_string().contains("tokens remain"));
    }

    #[tokio::test]
    async fn blockhash_cache_reuses_within_ttl_refetches_after() {
        use std::sync::atomic::Ordering;
        let kp = Keypair::new();
        let state = e2e_mock(unsigned_swap_fixture_b64(&kp), false);
        let base = spawn_mock(state.clone()).await;
        let mut ex =
            LiveExecutor::armed_with_signer(&e2e_config(&base, false), &e2e_loaded(kp)).unwrap();
        // Effectively infinite TTL: one RPC hit serves both calls.
        ex.blockhash_ttl_secs = u64::MAX;
        let first = ex.fresh_blockhash().await.unwrap();
        let second = ex.fresh_blockhash().await.unwrap();
        assert_eq!(first, second);
        assert_eq!(state.rpc_hits.load(Ordering::SeqCst), 1);
        // Zero TTL: every call refetches (staleness bound of zero).
        ex.blockhash_ttl_secs = 0;
        ex.fresh_blockhash().await.unwrap();
        ex.fresh_blockhash().await.unwrap();
        assert_eq!(state.rpc_hits.load(Ordering::SeqCst), 3);
    }

    /// Real-devnet connectivity check (ignored: needs network). Run with
    /// `cargo test -- --ignored devnet_blockhash` to prove the RPC plumbing
    /// against actual devnet before any M5 funds move.
    #[tokio::test]
    #[ignore]
    async fn devnet_blockhash_fetch_parses() {
        let rpc = RpcClient::new_for_test("https://api.devnet.solana.com");
        let (bh, height) = rpc
            .fetch_recent_blockhash()
            .await
            .expect("devnet RPC reachable");
        assert!(bh.len() >= 32, "blockhash looks real: {bh}");
        assert!(height > 0);
    }
}
