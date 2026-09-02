//! Audit trail (spec §6): every decision, order, fill, and breaker event is
//! appended to an immutable JSONL log. In paper mode this is the persistence
//! layer; the Postgres schema (sqlx) arrives with M5/M6 and replays from the
//! same records.

use crate::types::{ClosedTrade, Fill};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditRecord {
    /// A decision the engine made (entry accepted/rejected, exit, promotion).
    Decision {
        ts: DateTime<Utc>,
        mint: String,
        action: String,
        detail: String,
    },
    /// An order sent to the executor.
    Order {
        ts: DateTime<Utc>,
        order_id: String,
        mint: String,
        side: String,
        budget_or_qty: Decimal,
        ref_price: Decimal,
    },
    Fill {
        ts: DateTime<Utc>,
        order_id: String,
        mint: String,
        side: String,
        qty: Decimal,
        price_usd: Decimal,
        notional_usd: Decimal,
        fee_usd: Decimal,
    },
    TradeClosed(ClosedTrade),
    Breaker {
        ts: DateTime<Utc>,
        reason: String,
    },
}

/// Append-only JSONL audit log. Lines are flushed on every append so a crash
/// never loses the trail.
pub struct AuditLog {
    file: File,
    path: PathBuf,
}

impl AuditLog {
    pub fn open(path: &Path) -> std::io::Result<AuditLog> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(AuditLog {
            file,
            path: path.to_path_buf(),
        })
    }

    pub fn append(&mut self, record: &AuditRecord) -> std::io::Result<()> {
        let mut line = serde_json::to_string(record)
            .expect("audit record is serializable");
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.flush()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Convenience wrapper so callers don't need to build timestamps themselves.
pub fn fill_record(fill: &Fill) -> AuditRecord {
    AuditRecord::Fill {
        ts: fill.ts,
        order_id: fill.order_id.clone(),
        mint: fill.mint.clone(),
        side: format!("{:?}", fill.side).to_lowercase(),
        qty: fill.qty,
        price_usd: fill.price_usd,
        notional_usd: fill.notional_usd,
        fee_usd: fill.fee_usd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Fill, Side};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn ts(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + s, 0).unwrap()
    }

    #[test]
    fn appends_jsonl_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit").join("audit.jsonl");
        let mut log = AuditLog::open(&path).unwrap();

        log.append(&AuditRecord::Decision {
            ts: ts(0),
            mint: "MINT".into(),
            action: "entry".into(),
            detail: "accepted".into(),
        })
        .unwrap();

        let fill = Fill {
            order_id: "o1".into(),
            mint: "MINT".into(),
            side: Side::Buy,
            qty: dec!(10),
            price_usd: dec!(1),
            notional_usd: dec!(10),
            fee_usd: dec!(0.1),
            ts: ts(1),
        };
        log.append(&fill_record(&fill)).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"kind\":\"decision\""));
        assert!(lines[1].contains("\"kind\":\"fill\""));
        assert!(lines[1].contains("\"side\":\"buy\""));
    }
}
