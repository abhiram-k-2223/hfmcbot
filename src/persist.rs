//! Audit trail (spec §6): every decision, order, fill, and breaker event is
//! appended to an immutable JSONL log. In paper mode this is the persistence
//! layer; the Postgres schema (sqlx) arrives with M5/M6 and replays from the
//! same records.

use crate::risk::RiskState;
use crate::types::{ClosedTrade, Fill, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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
///
/// With `--features pg` an optional `PgMirror` can be attached: every record
/// is ALSO queued for Postgres (best-effort, non-blocking — a dead database
/// never stalls the file trail).
pub struct AuditLog {
    file: File,
    path: PathBuf,
    #[cfg(feature = "pg")]
    mirror: Option<crate::pg_audit::PgMirror>,
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
            #[cfg(feature = "pg")]
            mirror: None,
        })
    }

    /// Attach a Postgres mirror (only compiled with `--features pg`). Every
    /// subsequent `append` is queued for Postgres as well as the file.
    #[cfg(feature = "pg")]
    pub fn set_mirror(&mut self, mirror: crate::pg_audit::PgMirror) {
        self.mirror = Some(mirror);
    }

    pub fn append(&mut self, record: &AuditRecord) -> std::io::Result<()> {
        #[cfg(feature = "pg")]
        if let Some(m) = &self.mirror {
            m.log(record);
        }
        let mut line = serde_json::to_string(record).expect("audit record is serializable");
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.flush()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Crash-safe engine snapshot (M6). A single JSON document holding everything
/// needed to resume exactly: book, ledger, sequence counter, and breaker
/// state. Positions are stored sorted by mint so snapshots are deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineState {
    /// Schema version; loaders refuse anything they don't understand.
    pub version: u32,
    pub equity_usd: Decimal,
    pub deployed_usd: Decimal,
    pub positions: Vec<Position>,
    pub closed: Vec<ClosedTrade>,
    pub order_seq: u64,
    pub risk: RiskState,
}

impl EngineState {
    pub const VERSION: u32 = 1;
}

/// Atomically write a snapshot: the file at `path` is either the previous
/// complete snapshot or the new complete one — never a torn write. A crash
/// mid-save leaves the temp file behind (ignored on load) and the last good
/// snapshot intact.
pub fn save_state(path: &Path, state: &EngineState) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut buf = serde_json::to_vec(state).expect("engine state is serializable");
    buf.push(b'\n');
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.flush()?;
    }
    std::fs::rename(&tmp, path)
}

/// Load a snapshot, refusing unknown schema versions loudly rather than
/// resuming on a misinterpreted book.
pub fn load_state(path: &Path) -> std::io::Result<EngineState> {
    let text = std::fs::read_to_string(path)?;
    let state: EngineState = serde_json::from_str(&text).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("snapshot {} is corrupt: {e}", path.display()),
        )
    })?;
    if state.version != EngineState::VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "snapshot version {} != supported {}",
                state.version,
                EngineState::VERSION
            ),
        ));
    }
    Ok(state)
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
