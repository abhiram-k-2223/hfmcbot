//! Metrics + health endpoint (M0 leftover, spec §6).
//!
//! Minimal Prometheus-text `/metrics` + `/healthz` over a tokio TcpListener
//! with zero extra dependencies. Paper runs stay observable without pulling
//! a web framework.

use std::sync::{Arc, Mutex};

/// Point-in-time numbers scraped into Prometheus text format.
#[derive(Debug, Clone, Default)]
pub struct EngineSnapshot {
    pub equity_usd: String,
    pub deployed_usd: String,
    pub open_positions: usize,
    pub closed_trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub realized_pnl_usd: String,
    pub day_realized_pnl_usd: String,
    pub kill_switch: bool,
    /// Positions marked unsellable after `max_exit_attempts` failed exits.
    pub stuck_positions: usize,
}

/// Render Prometheus exposition format.
pub fn render_prometheus_text(s: &EngineSnapshot) -> String {
    let kill = if s.kill_switch { 1 } else { 0 };
    format!(
        "# HELP hfmcbot_equity_usd Realized equity in USD.\n\
         # TYPE hfmcbot_equity_usd gauge\n\
         hfmcbot_equity_usd {}\n\
         # HELP hfmcbot_deployed_usd USD locked in open positions.\n\
         # TYPE hfmcbot_deployed_usd gauge\n\
         hfmcbot_deployed_usd {}\n\
         # HELP hfmcbot_open_positions Open position count.\n\
         # TYPE hfmcbot_open_positions gauge\n\
         hfmcbot_open_positions {}\n\
         # HELP hfmcbot_closed_trades_total Closed trade count.\n\
         # TYPE hfmcbot_closed_trades_total counter\n\
         hfmcbot_closed_trades_total {}\n\
         # HELP hfmcbot_trade_wins_total Winning closed trades.\n\
         # TYPE hfmcbot_trade_wins_total counter\n\
         hfmcbot_trade_wins_total {}\n\
         # HELP hfmcbot_trade_losses_total Losing closed trades.\n\
         # TYPE hfmcbot_trade_losses_total counter\n\
         hfmcbot_trade_losses_total {}\n\
         # HELP hfmcbot_realized_pnl_usd Cumulative realized P&L in USD.\n\
         # TYPE hfmcbot_realized_pnl_usd counter\n\
         hfmcbot_realized_pnl_usd {}\n\
         # HELP hfmcbot_day_realized_pnl_usd Current UTC-day realized P&L.\n\
         # TYPE hfmcbot_day_realized_pnl_usd gauge\n\
         hfmcbot_day_realized_pnl_usd {}\n\
         # HELP hfmcbot_kill_switch Circuit-breaker state (1 = tripped).\n\
         # TYPE hfmcbot_kill_switch gauge\n\
         hfmcbot_kill_switch {}\n\
         # HELP hfmcbot_stuck_positions Positions marked unsellable.\n\
         # TYPE hfmcbot_stuck_positions gauge\n\
         hfmcbot_stuck_positions {}\n",
        s.equity_usd,
        s.deployed_usd,
        s.open_positions,
        s.closed_trades,
        s.wins,
        s.losses,
        s.realized_pnl_usd,
        s.day_realized_pnl_usd,
        kill,
        s.stuck_positions,
    )
}

/// Install a panic hook that routes panics through `tracing::error`.
pub fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(panic = %info, "hfmcbot panic");
        default(info);
    }));
}

/// Serve `/metrics` + `/healthz` forever. `snapshot` is polled per scrape.
pub async fn serve_metrics(
    addr: String,
    snapshot: Arc<Mutex<EngineSnapshot>>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "metrics endpoint listening on /metrics + /healthz");
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let snapshot = snapshot.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            let (status, content_type, body) = match path {
                "/healthz" => ("200 OK", "text/plain", "ok\n".to_string()),
                "/metrics" => {
                    let text = snapshot
                        .lock()
                        .map(|s| render_prometheus_text(&s))
                        .unwrap_or_default();
                    ("200 OK", "text/plain; version=0.0.4", text)
                }
                _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

/// Heartbeat: periodic `tracing::info!` so paper runs show liveness in logs.
pub fn spawn_heartbeat(interval_secs: u64) {
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
        loop {
            t.tick().await;
            tracing::info!("hfmcbot heartbeat: alive");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> EngineSnapshot {
        EngineSnapshot {
            equity_usd: "50000".into(),
            deployed_usd: "0".into(),
            open_positions: 1,
            closed_trades: 2,
            wins: 1,
            losses: 1,
            realized_pnl_usd: "123.45".into(),
            day_realized_pnl_usd: "-10".into(),
            kill_switch: false,
            stuck_positions: 0,
        }
    }

    #[test]
    fn renders_prometheus_gauges_and_counters() {
        let text = render_prometheus_text(&snap());
        assert!(text.contains("hfmcbot_equity_usd 50000"));
        assert!(text.contains("hfmcbot_open_positions 1"));
        assert!(text.contains("hfmcbot_closed_trades_total 2"));
        assert!(text.contains("hfmcbot_kill_switch 0"));
    }

    #[test]
    fn kill_switch_renders_one_when_tripped() {
        let mut s = snap();
        s.kill_switch = true;
        assert!(render_prometheus_text(&s).contains("hfmcbot_kill_switch 1"));
    }

    /// The endpoint actually serves: bind an ephemeral port, GET /metrics +
    /// /healthz over a raw socket, assert status lines and bodies.
    #[tokio::test]
    async fn serves_metrics_and_healthz_over_http() {
        // Bind ephemeral port first so the test never collides.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let snap = Arc::new(Mutex::new(snap()));
        let server = tokio::spawn(serve_metrics(addr.clone(), snap.clone()));

        async fn get(addr: &str, path: &str) -> String {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            sock.write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut out = Vec::new();
            sock.read_to_end(&mut out).await.unwrap();
            String::from_utf8_lossy(&out).into_owned()
        }

        // Retry briefly: the server task needs a moment to bind.
        let mut metrics = String::new();
        for _ in 0..50 {
            match tokio::net::TcpStream::connect(&addr).await {
                Ok(_) => {
                    metrics = get(&addr, "/metrics").await;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
        assert!(metrics.starts_with("HTTP/1.1 200 OK"), "got: {metrics}");
        assert!(metrics.contains("hfmcbot_equity_usd 50000"));

        let health = get(&addr, "/healthz").await;
        assert!(health.starts_with("HTTP/1.1 200 OK"), "got: {health}");
        assert!(health.ends_with("ok\n"));

        let missing = get(&addr, "/nope").await;
        assert!(missing.starts_with("HTTP/1.1 404"), "got: {missing}");

        server.abort();
    }
}
