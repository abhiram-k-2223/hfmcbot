//! Postgres audit mirror (roadmap, `--features pg`): every JSONL audit record
//! is ALSO inserted into Postgres for querying (P&L by mint, exit-reason
//! histograms, stuck-position history).
//!
//! Opt-in by design: the JSONL log stays the primary trail (crash-safe,
//! zero-dependency, always on). Postgres is a queryable mirror, never the
//! source of truth — so the mirror is best-effort by construction:
//! - `log()` is sync + non-blocking (`try_send` into a bounded channel); a
//!   slow or dead database NEVER backpressures trading. Overflow drops +
//!   counts + logs loudly (`dropped()` exposes the count for `/metrics`).
//! - The background worker owns the ONLY connection; failed inserts are
//!   logged and dropped, never retried (retrying audit writes could reorder
//!   the mirror; the JSONL file remains complete).
//! - TLS: `tokio-postgres` connects with `NoTls` here — run Postgres on
//!   localhost, a Unix socket, or behind a TLS-terminating proxy. Direct
//!   cleartext over the open internet is an operator error, not a default.
//!
//! Schema is created with `CREATE TABLE IF NOT EXISTS` by `ensure_schema`,
//! so a fresh database just works and upgrades never wipe history.

use crate::persist::AuditRecord;
use chrono::{DateTime, Utc};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Mirror schema. One row per audit record: indexed timestamp + mint carry
/// the queries operators actually run; the full record rides as JSONB so
/// schema evolution never needs a migration for new record shapes.
pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS hfmcbot_audit (
  id      BIGSERIAL PRIMARY KEY,
  ts      TIMESTAMPTZ NOT NULL,
  kind    TEXT        NOT NULL,
  mint    TEXT,
  payload JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS hfmcbot_audit_ts_idx   ON hfmcbot_audit (ts);
CREATE INDEX IF NOT EXISTS hfmcbot_audit_mint_idx ON hfmcbot_audit (mint, id);
CREATE INDEX IF NOT EXISTS hfmcbot_audit_kind_idx ON hfmcbot_audit (kind);
"#;

/// Depth of the mirror channel. 10K records of headroom: at the bot's event
/// rates (tens/sec worst case) this absorbs multi-minute DB stalls; beyond
/// that, dropping + counting loudly beats blocking the hot path.
pub const MIRROR_CHANNEL_DEPTH: usize = 10_000;

/// Redact a Postgres DSN for logs: `postgres://user:pass@host/db` →
/// `postgres://user:***@host/db`. Passwords never appear in boot logs.
/// Unknown shapes pass through UNCHANGED (fail-open display, fail-closed
/// connection — the driver still validates the real string).
pub fn redacted_dsn(dsn: &str) -> String {
    let Some(at) = dsn.rfind('@') else {
        return dsn.to_string();
    };
    let (head, tail) = dsn.split_at(at);
    match head.rfind(':') {
        Some(ci) if head[..ci].contains("://") => format!("{}:***{tail}", &head[..ci]),
        _ => dsn.to_string(),
    }
}

/// Split a record into its indexed columns. The `kind` strings match the
/// serde tags on `AuditRecord` exactly (one vocabulary, two encodings).
pub fn record_parts(record: &AuditRecord) -> (DateTime<Utc>, &'static str, Option<String>) {
    match record {
        AuditRecord::Decision { ts, mint, .. } => (*ts, "decision", Some(mint.clone())),
        AuditRecord::Order { ts, mint, .. } => (*ts, "order", Some(mint.clone())),
        AuditRecord::Fill { ts, mint, .. } => (*ts, "fill", Some(mint.clone())),
        AuditRecord::TradeClosed(t) => (t.closed_at, "trade_closed", Some(t.mint.clone())),
        AuditRecord::Breaker { ts, .. } => (*ts, "breaker", None),
    }
}

struct MirrorItem {
    ts: DateTime<Utc>,
    kind: &'static str,
    mint: Option<String>,
    payload: String,
}

/// Cloneable Postgres mirror handle. `log()` works with or without a live
/// worker — records queue while the worker runs, drop + count when the
/// channel is full or gone.
#[derive(Debug, Clone)]
pub struct PgMirror {
    tx: Option<tokio::sync::mpsc::Sender<MirrorItem>>,
    dropped: Arc<AtomicU64>,
}

impl PgMirror {
    /// Disabled mirror (no DSN): `log()` is a no-op, `dropped()` is 0.
    pub fn disabled() -> PgMirror {
        PgMirror {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Connect + ensure schema, then spawn the drain worker onto the current
    /// runtime. Boot-loud on failure (the caller decides whether a dead
    /// mirror is fatal — live mode refuses, paper warns).
    pub async fn connect(dsn: &str) -> Result<PgMirror, String> {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pg connect {} failed: {e}", redacted_dsn(dsn)))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "pg connection task ended");
            }
        });
        client
            .batch_execute(SCHEMA_SQL)
            .await
            .map_err(|e| format!("pg ensure_schema failed: {e}"))?;
        let (tx, rx) = tokio::sync::mpsc::channel(MIRROR_CHANNEL_DEPTH);
        let mirror = PgMirror {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
        };
        tokio::spawn(run_worker(client, rx, mirror.dropped.clone()));
        Ok(mirror)
    }

    /// Queue one record for mirroring. Sync, non-blocking, infallible: full
    /// or dead channel → drop, count, and ERROR-log (audit loss is always
    /// loud; the JSONL file still has the record).
    pub fn log(&self, record: &AuditRecord) {
        let Some(tx) = &self.tx else { return };
        let (ts, kind, mint) = record_parts(record);
        let payload = serde_json::to_string(record).expect("audit record is serializable");
        match tx.try_send(MirrorItem {
            ts,
            kind,
            mint,
            payload,
        }) {
            Ok(()) => {}
            Err(_) => {
                let n = self.dropped.fetch_add(1, Ordering::SeqCst) + 1;
                tracing::error!(
                    kind,
                    dropped_total = n,
                    "pg mirror channel full/dead — audit record dropped (JSONL still complete)"
                );
            }
        }
    }

    /// Records dropped since boot (0 when disabled or healthy). Wire to
    /// alerting/metrics: a nonzero count means the mirror diverged.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }
}

async fn run_worker(
    client: tokio_postgres::Client,
    mut rx: tokio::sync::mpsc::Receiver<MirrorItem>,
    dropped: Arc<AtomicU64>,
) {
    while let Some(item) = rx.recv().await {
        let res = client
            .execute(
                "INSERT INTO hfmcbot_audit (ts, kind, mint, payload) VALUES ($1, $2, $3, $4::jsonb)",
                &[&item.ts, &item.kind, &item.mint, &item.payload],
            )
            .await;
        if let Err(e) = res {
            // Best-effort by design (see module docs): log + count + move on.
            // Retrying here could reorder the mirror behind the JSONL truth.
            let n = dropped.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::error!(
                kind = item.kind,
                error = %e,
                dropped_total = n,
                "pg mirror insert failed — record dropped (JSONL still complete)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::fill_record;
    use crate::types::{Fill, Side};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn dsn_redaction_hides_password_keeps_rest() {
        assert_eq!(
            redacted_dsn("postgres://bot:s3cret@db.internal:5432/hfmc"),
            "postgres://bot:***@db.internal:5432/hfmc"
        );
        // No password present: untouched.
        assert_eq!(
            redacted_dsn("postgres://bot@db.internal/hfmc"),
            "postgres://bot@db.internal/hfmc"
        );
        assert_eq!(redacted_dsn(""), "");
        // Never let a password-shaped string through even in odd shapes:
        // anything with user:secret@ is redacted.
        let r = redacted_dsn("postgresql://u:p%40ss@h/db?sslmode=require");
        assert!(!r.contains("p%40ss"), "leaked: {r}");
        assert!(r.contains(":***@"), "over-redacted: {r}");
    }

    #[test]
    fn record_parts_match_serde_tags() {
        let ts = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let fill = Fill {
            order_id: "o1".into(),
            mint: "MINT".into(),
            side: Side::Buy,
            qty: dec!(10),
            price_usd: dec!(1),
            notional_usd: dec!(10),
            fee_usd: dec!(0.1),
            ts,
        };
        let rec = fill_record(&fill);
        // The kind MUST equal the serde tag or queries and JSONL disagree.
        let json = serde_json::to_value(&rec).unwrap();
        let (pts, kind, mint) = record_parts(&rec);
        assert_eq!(json["kind"], kind);
        assert_eq!(pts, ts);
        assert_eq!(mint.as_deref(), Some("MINT"));
        let br = AuditRecord::Breaker {
            ts,
            reason: "trip".into(),
        };
        let (_, bkind, bmint) = record_parts(&br);
        assert_eq!(serde_json::to_value(&br).unwrap()["kind"], bkind);
        assert_eq!(bmint, None);
    }

    #[test]
    fn schema_creates_the_queried_table_and_indexes() {
        for needle in [
            "hfmcbot_audit",
            "ts      TIMESTAMPTZ",
            "payload JSONB",
            "hfmcbot_audit_ts_idx",
            "hfmcbot_audit_mint_idx",
            "hfmcbot_audit_kind_idx",
            "IF NOT EXISTS",
        ] {
            assert!(SCHEMA_SQL.contains(needle), "schema missing {needle}");
        }
    }

    #[test]
    fn disabled_mirror_logs_nothing_drops_nothing() {
        let m = PgMirror::disabled();
        let ts = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        m.log(&AuditRecord::Breaker {
            ts,
            reason: "x".into(),
        });
        assert_eq!(m.dropped(), 0);
    }
}
