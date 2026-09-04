//! Operator alerting (M6): loud, rate-limited, never load-bearing.
//!
//! Every alert is ALWAYS logged via `tracing::error` (the reliable channel —
//! tail the logs or ship them to Loki). When `HFM_ALERT_WEBHOOK_URL` is set,
//! alerts are additionally POSTed as JSON to that URL by a background worker.
//!
//! Design constraints (all deliberate):
//! - `fire()` is sync + non-blocking: it logs and drops the alert into an
//!   mpsc channel. Alerting must never stall the hot path or a sync risk
//!   check, and a dead webhook must never backpressure trading.
//! - Rate-limited per kind (`HFM_ALERT_MIN_SECS`, default 300): a flapping
//!   breaker pages once per window, not once per event.
//! - The worker is best-effort: failed POSTs are logged and dropped. The
//!   audit trail + logs remain the source of truth, never the webhook.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What happened. Serialized into the webhook payload as snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertKind {
    /// Manual kill switch engaged at boot (real-money guardrail on).
    KillSwitch,
    /// Daily-loss breaker tripped mid-run (entries halted).
    DailyLossTrip,
    /// A position exhausted `max_exit_attempts` — funds stuck on-chain.
    PositionStuck,
    /// An exit failed with UNKNOWN order state — reconcile, don't resubmit.
    TransportUnknown,
    /// A crash-safe state snapshot failed to write.
    SnapshotFailed,
}

impl AlertKind {
    fn as_str(self) -> &'static str {
        match self {
            AlertKind::KillSwitch => "kill_switch",
            AlertKind::DailyLossTrip => "daily_loss_trip",
            AlertKind::PositionStuck => "position_stuck",
            AlertKind::TransportUnknown => "transport_unknown",
            AlertKind::SnapshotFailed => "snapshot_failed",
        }
    }
}

/// One alert delivery. The worker POSTs the JSON form.
#[derive(Debug, Clone)]
pub struct Alert {
    pub kind: AlertKind,
    pub detail: String,
    pub at: DateTime<Utc>,
}

impl Alert {
    fn payload(&self, service: &str) -> serde_json::Value {
        serde_json::json!({
            "service": service,
            "kind": self.kind.as_str(),
            "detail": self.detail,
            "at": self.at.to_rfc3339(),
        })
    }
}

#[derive(Debug)]
struct AlerterState {
    last_sent: HashMap<AlertKind, Instant>,
}

/// Cloneable alert handle. The webhook receiver is owned by whoever spawns
/// `run_worker` (main); `fire()` works with or without a worker — log-only
/// when the channel is gone.
#[derive(Debug, Clone)]
pub struct Alerter {
    tx: Option<tokio::sync::mpsc::UnboundedSender<Alert>>,
    min_interval: Duration,
    state: Arc<Mutex<AlerterState>>,
}

impl Alerter {
    /// Build an alerter + its delivery channel. `webhook: false` (no URL
    /// configured) yields a log-only alerter whose `fire()` still enforces
    /// rate limits and answers whether this alert was new.
    pub fn new(webhook: bool, min_interval_secs: u64) -> (Alerter, Option<AlertRx>) {
        let (tx, rx) = if webhook {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (Some(tx), Some(AlertRx { rx }))
        } else {
            (None, None)
        };
        (
            Alerter {
                tx,
                min_interval: Duration::from_secs(min_interval_secs),
                state: Arc::new(Mutex::new(AlerterState {
                    last_sent: HashMap::new(),
                })),
            },
            rx,
        )
    }

    /// Fire an alert. ALWAYS logs; queues for webhook delivery unless this
    /// kind already fired inside the rate window. Returns true when this
    /// alert was new (logged + queued), false when rate-limited away.
    /// Never blocks, never fails.
    pub fn fire(&self, kind: AlertKind, detail: impl Into<String>) -> bool {
        let detail = detail.into();
        let now = Instant::now();
        {
            let mut st = self.state.lock().unwrap();
            if let Some(last) = st.last_sent.get(&kind) {
                if now.saturating_duration_since(*last) < self.min_interval {
                    return false;
                }
            }
            st.last_sent.insert(kind, now);
        }
        tracing::error!(kind = kind.as_str(), %detail, "ALERT");
        if let Some(ref tx) = self.tx {
            // Receiver gone (no worker / shutting down): the log line above
            // is the delivery. Drop, don't panic.
            let _ = tx.send(Alert {
                kind,
                detail,
                at: Utc::now(),
            });
        }
        true
    }
}

/// Receiving end of the delivery channel. Owned by the webhook worker.
#[derive(Debug)]
pub struct AlertRx {
    rx: tokio::sync::mpsc::UnboundedReceiver<Alert>,
}

impl AlertRx {
    /// Non-blocking receive (tests + diagnostics).
    pub fn try_recv(&mut self) -> Result<Alert, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

/// Webhook worker (spawned once in main when a URL is configured): POSTs
/// each alert as JSON until the sender side drops. Failed POSTs are logged
/// and dropped — the worker never retries (rate limits already dedupe) and
/// never panics.
pub async fn run_worker(mut rx: AlertRx, url: String, service: String) {
    let http = reqwest::Client::new();
    while let Some(alert) = rx.rx.recv().await {
        let body = alert.payload(&service);
        if let Err(e) = http.post(url.as_str()).json(&body).send().await {
            tracing::warn!(
                kind = alert.kind.as_str(),
                error = %e,
                "alert webhook POST failed (dropped; logs remain source of truth)"
            );
        }
    }
    tracing::info!("alert worker exiting (all senders dropped)");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_only() -> Alerter {
        Alerter::new(false, 300).0
    }

    #[test]
    fn first_fire_is_new_second_is_rate_limited() {
        let a = log_only();
        assert!(a.fire(AlertKind::DailyLossTrip, "day -$5000"));
        assert!(!a.fire(AlertKind::DailyLossTrip, "day -$5001"));
        // Other kinds have independent windows.
        assert!(a.fire(AlertKind::PositionStuck, "MINT stuck"));
    }

    #[test]
    fn zero_window_never_limits() {
        let (a, _) = Alerter::new(false, 0);
        assert!(a.fire(AlertKind::KillSwitch, "a"));
        assert!(a.fire(AlertKind::KillSwitch, "b"));
    }

    #[test]
    fn alert_payload_shape() {
        let at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let p = Alert {
            kind: AlertKind::TransportUnknown,
            detail: "bundle poll timed out".into(),
            at,
        }
        .payload("hfmcbot-test");
        assert_eq!(p["service"], "hfmcbot-test");
        assert_eq!(p["kind"], "transport_unknown");
        assert_eq!(p["detail"], "bundle poll timed out");
        assert!(p["at"].as_str().unwrap().starts_with("2023-11-14"));
    }

    #[tokio::test]
    async fn webhook_mode_queues_and_worker_posts() {
        // Tiny capture server: reads one POST body, stores it, replies {}.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut chunk = vec![0u8; 4096];
            let mut data = Vec::new();
            let head_end = loop {
                let n = sock.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                data.extend_from_slice(&chunk[..n]);
                if let Some(p) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    break p + 4;
                }
            };
            let head = String::from_utf8_lossy(&data[..head_end]).to_string();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("Content-Length:")
                        .or_else(|| l.strip_prefix("content-length:"))
                })
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            while data.len() < head_end + len {
                let n = sock.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&chunk[..n]);
            }
            seen2
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&data[head_end..]).to_string());
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            let _ = sock.write_all(resp.as_bytes()).await;
        });

        let (alerter, rx) = Alerter::new(true, 300);
        let url = format!("http://{addr}/hook");
        let worker = tokio::spawn(run_worker(rx.unwrap(), url, "hfmcbot-test".into()));
        assert!(alerter.fire(AlertKind::PositionStuck, "MINT9 stuck $1200"));
        // Rate-limited twin never reaches the wire.
        assert!(!alerter.fire(AlertKind::PositionStuck, "MINT9 stuck $1200"));
        drop(alerter); // closes the channel → worker exits after delivery
        worker.await.unwrap();
        let bodies = seen.lock().unwrap();
        assert_eq!(bodies.len(), 1, "exactly one POST: {bodies:?}");
        let v: serde_json::Value = serde_json::from_str(&bodies[0]).unwrap();
        assert_eq!(v["kind"], "position_stuck");
        assert_eq!(v["service"], "hfmcbot-test");
    }
}
