//! Key management (spec §5.1, M0 leftover).
//!
//! Loads the operator keypair from env (`HFM_SECRET_KEY`, fallback `SECRET_KEY`)
//! and NEVER logs or exposes the secret itself — callers only ever see the
//! base58 pubkey + which var it came from.
//!
//! Accepted formats (all standard Solana wallet exports):
//! - base58-encoded 64-byte secret key (`solana-keygen` / `bs58` export),
//! - base58-encoded 32-byte seed,
//! - JSON array of 64 or 32 numbers (Solana CLI `id.json` / `keypair.json`).

use solana_keypair::{keypair_from_seed, Keypair, Signer};

/// Which env var the key came from (safe to log — never the value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    HfmSecretKey,
    SecretKey,
}

impl KeySource {
    pub fn var_name(&self) -> &'static str {
        match self {
            KeySource::HfmSecretKey => "HFM_SECRET_KEY",
            KeySource::SecretKey => "SECRET_KEY",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("invalid key in {var}: {reason} (value redacted)")]
    Invalid { var: &'static str, reason: String },
}

/// Loaded keypair + safe-to-log identity.
pub struct LoadedKey {
    pub keypair: Keypair,
    pub source: KeySource,
    /// Base58 pubkey — safe to log, emit in metrics, include in audit.
    pub pubkey_base58: String,
}

/// Load from env if present. Returns `Ok(None)` when neither var is set
/// (valid in paper mode — M4+ live boot requires `Some`).
pub fn load_keypair_opt() -> Result<Option<LoadedKey>, KeyError> {
    for source in [KeySource::HfmSecretKey, KeySource::SecretKey] {
        if let Ok(raw) = std::env::var(source.var_name()) {
            if raw.trim().is_empty() {
                continue;
            }
            let keypair = parse_secret_key(raw.trim(), source.var_name())?;
            let pubkey_base58 = keypair.pubkey().to_string();
            // Deliberately log-safe: pubkey + source only, never the secret.
            tracing::info!(
                pubkey = %pubkey_base58,
                source = source.var_name(),
                "operator keypair loaded"
            );
            return Ok(Some(LoadedKey {
                keypair,
                source,
                pubkey_base58,
            }));
        }
    }
    Ok(None)
}

/// Parse one secret-key string. Error messages describe the problem shape
/// (length, encoding) but NEVER echo the secret value.
pub fn parse_secret_key(raw: &str, var: &'static str) -> Result<Keypair, KeyError> {
    let invalid = |reason: &str| KeyError::Invalid {
        var,
        reason: reason.to_string(),
    };

    // JSON array format: "[1,2,...]" with 64 or 32 u8 entries.
    if raw.starts_with('[') {
        let nums: Vec<u8> = serde_json::from_str(raw)
            .map_err(|e| invalid(&format!("invalid JSON array: {e}")))?;
        return bytes_to_keypair(&nums, var);
    }

    // Base58 format (default Solana export).
    let bytes = bs58::decode(raw)
        .into_vec()
        .map_err(|e| invalid(&format!("invalid base58: {e}")))?;
    bytes_to_keypair(&bytes, var)
}

fn bytes_to_keypair(bytes: &[u8], var: &'static str) -> Result<Keypair, KeyError> {
    let invalid = |reason: String| KeyError::Invalid { var, reason };
    match bytes.len() {
        64 => Keypair::try_from(bytes).map_err(|e| invalid(format!("invalid 64-byte key: {e}"))),
        32 => keypair_from_seed(bytes).map_err(|e| invalid(format!("invalid 32-byte seed: {e}"))),
        n => Err(invalid(format!(
            "expected 64-byte key or 32-byte seed, got {n} bytes"
        ))),
    }
}

/// Base58 pubkey for a keypair — the only key-derived string that may be logged.
pub fn pubkey_base58(kp: &Keypair) -> String {
    kp.pubkey().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b58(bytes: &[u8]) -> String {
        bs58::encode(bytes).into_string()
    }

    #[test]
    fn parses_base58_64_byte_key() {
        let kp = Keypair::new();
        let raw = b58(&kp.to_bytes());
        let parsed = parse_secret_key(&raw, "TEST").unwrap();
        assert_eq!(parsed.pubkey(), kp.pubkey());
        assert_eq!(pubkey_base58(&parsed), kp.pubkey().to_string());
    }

    #[test]
    fn parses_base58_32_byte_seed() {
        let kp = Keypair::new();
        // Seed = first 32 bytes of the secret key.
        let seed = &kp.to_bytes()[..32];
        let raw = b58(seed);
        let parsed = parse_secret_key(&raw, "TEST").unwrap();
        assert_eq!(parsed.pubkey(), kp.pubkey());
    }

    #[test]
    fn parses_json_array_formats() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json64 = format!("[{}]", bytes.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(","));
        assert_eq!(parse_secret_key(&json64, "TEST").unwrap().pubkey(), kp.pubkey());

        let seed_json = format!(
            "[{}]",
            bytes[..32].iter().map(|b| b.to_string()).collect::<Vec<_>>().join(",")
        );
        assert_eq!(parse_secret_key(&seed_json, "TEST").unwrap().pubkey(), kp.pubkey());
    }

    #[test]
    fn rejects_garbage_without_echoing_secret() {
        for bad in ["", "!!!not-base58!!!", "[1,2,3]", "AAAA", "[1,2,999]"] {
            let raw = if bad.is_empty() { "zzz" } else { bad };
            let err = parse_secret_key(raw, "HFM_SECRET_KEY").unwrap_err().to_string();
            // The secret value must never appear in the error.
            assert!(!err.contains(raw) || raw.len() < 4, "error leaked secret: {err}");
            assert!(err.contains("HFM_SECRET_KEY"));
        }
        // Wrong length base58 (e.g. 10 bytes) rejected with length info.
        let short = b58(&[1u8; 10]);
        let err = parse_secret_key(&short, "TEST").unwrap_err().to_string();
        assert!(err.contains("10 bytes"));
    }

    #[test]
    fn env_loading_prefers_hfm_var_and_handles_absent() {
        // Neither set → None (valid in paper mode).
        let guard = EnvGuard::clear(["HFM_SECRET_KEY", "SECRET_KEY"]);
        assert!(load_keypair_opt().unwrap().is_none());
        drop(guard);

        let kp = Keypair::new();
        let raw = b58(&kp.to_bytes());
        let _guard = EnvGuard::set("HFM_SECRET_KEY", &raw);
        let loaded = load_keypair_opt().unwrap().unwrap();
        assert_eq!(loaded.source, KeySource::HfmSecretKey);
        assert_eq!(loaded.pubkey_base58, kp.pubkey().to_string());
    }

    /// Test helper that saves/restores env vars (tests run in threads).
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }
    impl EnvGuard {
        fn clear(vars: [&'static str; 2]) -> EnvGuard {
            let saved = vars.iter().map(|v| (*v, std::env::var(v).ok())).collect();
            for v in vars {
                unsafe { std::env::remove_var(v) };
            }
            EnvGuard { saved }
        }
        fn set(var: &'static str, val: &str) -> EnvGuard {
            let saved = vec![(var, std::env::var(var).ok())];
            unsafe { std::env::remove_var("HFM_SECRET_KEY") };
            unsafe { std::env::remove_var("SECRET_KEY") };
            unsafe { std::env::set_var(var, val) };
            EnvGuard { saved }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in self.saved.drain(..) {
                unsafe {
                    match v {
                        Some(val) => std::env::set_var(k, val),
                        None => std::env::remove_var(k),
                    }
                }
            }
        }
    }
}
