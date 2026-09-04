//! Devnet funds-moving proof (the M4-deferred item): end-to-end exercise of
//! the REAL send path — keypair → faucet airdrop → balance read → blockhash →
//! local signing (`build_tip_tx_b64`, the same builder the Jito bundles use)
//! → `sendTransaction` → `confirm_signature` — against devnet.
//!
//! SAFETY:
//! - Refuses to run unless `HFM_RPC_URL` contains "devnet". There is no flag
//!   to override this: mainnet funds move only through the bot itself.
//! - The keypair is THROWAWAY (generated fresh every run, never saved, never
//!   funded with real SOL). It only ever holds devnet faucet SOL.
//! - The transfer is self-to-self (5,000 lamports ≈ dust): it proves signing,
//!   submission, and confirmation move value on-chain without directional
//!   risk or a counterparty.
//!
//! Run: `HFM_RPC_URL=https://api.devnet.solana.com cargo run --bin devnet_trade`

use hfmcbot::config::Config;
use hfmcbot::live::RpcClient;
use solana_keypair::{Keypair, Signer};

const DEVNET_MARKER: &str = "devnet";
const AIRDROP_LAMPORTS: u64 = 1_000_000_000; // 1 devnet SOL — faucet minimum useful
const SELF_SEND_LAMPORTS: u64 = 5_000; // dust: proves movement, risks nothing
const CONFIRM_TIMEOUT_SECS: u64 = 90;

#[tokio::main]
async fn main() -> Result<(), String> {
    let rpc_url = std::env::var("HFM_RPC_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "https://api.devnet.solana.com".to_string());
    if !rpc_url.contains(DEVNET_MARKER) {
        return Err(format!(
            "refusing: HFM_RPC_URL={rpc_url:?} does not contain {DEVNET_MARKER:?} — this binary only moves devnet SOL"
        ));
    }
    let mut cfg = Config::paper_defaults();
    cfg.rpc_url = rpc_url.clone();
    let rpc = RpcClient::new(&cfg);

    // Throwaway identity: fresh every run, printed once so the operator can
    // watch it on a devnet explorer. Never written to disk.
    let kp = Keypair::new();
    let pubkey = kp.pubkey().to_string();
    println!("throwaway pubkey: {pubkey}");
    println!("rpc: {rpc_url}");

    // Fund it: the public faucet rate-limits (429s) — retry patiently, fail
    // loudly with the exact recovery (wait, rerun) instead of proceeding dry.
    let mut airdrop_sig = String::new();
    for attempt in 1..=6u32 {
        match rpc.request_airdrop(&pubkey, AIRDROP_LAMPORTS).await {
            Ok(sig) => {
                airdrop_sig = sig;
                break;
            }
            Err(e) => {
                eprintln!("airdrop attempt {attempt}/6 failed: {e}");
                if attempt == 6 {
                    return Err(format!(
                        "faucet exhausted after 6 attempts ({e}) — wait ~1h for the rate limit, then rerun"
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            }
        }
    }
    println!("airdrop sig: {airdrop_sig}");
    rpc.confirm_signature(&airdrop_sig, CONFIRM_TIMEOUT_SECS)
        .await
        .map_err(|e| format!("airdrop never confirmed: {e}"))?;

    let before = rpc
        .get_balance(&pubkey)
        .await
        .map_err(|e| format!("balance read failed: {e}"))?;
    println!("balance before: {before} lamports");
    if before < AIRDROP_LAMPORTS / 2 {
        return Err(format!(
            "balance {before} far below the {AIRDROP_LAMPORTS} airdrop — something is wrong, refusing to send"
        ));
    }

    // Same builder the Jito bundles use, aimed back at ourselves.
    let (blockhash, _) = rpc
        .fetch_recent_blockhash()
        .await
        .map_err(|e| format!("blockhash fetch failed: {e}"))?;
    let self_pubkey = kp.pubkey();
    let tx_b64 = hfmcbot::live::build_tip_tx_b64(&kp, &self_pubkey, SELF_SEND_LAMPORTS, &blockhash)
        .map_err(|e| format!("local signing failed: {e}"))?;
    let sig = rpc
        .send_transaction(&tx_b64)
        .await
        .map_err(|e| format!("sendTransaction failed: {e}"))?;
    println!("self-send sig: {sig}");
    rpc.confirm_signature(&sig, CONFIRM_TIMEOUT_SECS)
        .await
        .map_err(|e| format!("self-send never confirmed: {e}"))?;

    let after = rpc
        .get_balance(&pubkey)
        .await
        .map_err(|e| format!("post-send balance read failed: {e}"))?;
    println!("balance after:  {after} lamports");
    println!("DEVNET E2E OK: signed + submitted + confirmed a value-moving transaction; fee+rent delta = {} lamports", before.saturating_sub(after));
    Ok(())
}
