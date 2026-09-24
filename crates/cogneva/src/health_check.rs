//! `cogneva --health-check` — the probe the image's `HEALTHCHECK` runs.
//!
//! One HTTP request against the gateway port, then exit: zero when the
//! application reports ready, non-zero otherwise. Container runtimes (docker,
//! podman) read the exit code to mark the container healthy or unhealthy.
//! Kubernetes ignores it and probes the same path with its own
//! liveness/readiness probes.
//!
//! The probe must not boot the application. It runs inside a container that is
//! already serving on that port, so a full boot ends in `bind failed` — an exit
//! code that says "unhealthy" about a container that works, and names the wrong
//! cause while doing it.
//!
//! Readiness, not liveness: a container that cannot serve traffic yet is not a
//! healthy container, which is what the runtime asks about.

use crate::config_loader;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::time::Duration;

/// Probe target. Kept off the business-request metric surface as an
/// infrastructure route — see the assertion in the tests below.
const PROBE_PATH: &str = "/health/ready";
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// A status line longer than this is not a status line; stop reading rather
/// than block until the timeout.
const STATUS_LINE_LIMIT: usize = 512;

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Same resolution as the server: the config stack, env overrides included,
    // so the probe follows `COGNEVA_HTTP_PORT` and a moved `gateway.http_port`.
    let port = config_loader::load().gateway.http_port;
    if port == 0 {
        // The default config leaves the port unset; the server would bind an
        // ephemeral one. Say that, instead of reporting a refused connection to
        // port 0 as if something had been listening there.
        return Err("health check failed: gateway.http_port is not configured".into());
    }
    let status = probe(port)
        .map_err(|e| format!("health check failed: {PROBE_PATH} on port {port}: {e}"))?;
    if status == 200 {
        Ok(())
    } else {
        Err(format!("health check failed: {PROBE_PATH} on port {port} returned {status}").into())
    }
}

/// `GET PROBE_PATH` on the loopback port, returning the response status code.
fn probe(port: u16) -> std::io::Result<u16> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))?;
    stream.set_read_timeout(Some(PROBE_TIMEOUT))?;
    stream.set_write_timeout(Some(PROBE_TIMEOUT))?;
    let request =
        format!("GET {PROBE_PATH} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let status_line = read_status_line(&mut stream)?;
    parse_status_code(&status_line).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected status line: {status_line:?}"),
        )
    })
}

/// Read through the end of the HTTP status line. One byte at a time is fine for
/// a single loopback line, and it stops at the line break instead of waiting for
/// the connection to close.
fn read_status_line(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    while line.len() < STATUS_LINE_LIMIT {
        if stream.read(&mut byte)? == 0 {
            break;
        }
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&line).trim().to_string())
}

/// `HTTP/1.1 200 OK` → `200`. Pure, so the accepted response shape is testable.
fn parse_status_code(status_line: &str) -> Option<u16> {
    let mut parts = status_line.split_whitespace();
    if !parts.next()?.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn status_code_comes_from_the_second_token() {
        assert_eq!(parse_status_code("HTTP/1.1 200 OK"), Some(200));
        assert_eq!(
            parse_status_code("HTTP/1.1 503 Service Unavailable"),
            Some(503)
        );
        assert_eq!(parse_status_code("HTTP/1.0 404 Not Found"), Some(404));
    }

    #[test]
    fn a_line_that_is_not_http_has_no_status_code() {
        assert_eq!(parse_status_code(""), None);
        assert_eq!(parse_status_code("200 OK"), None);
        assert_eq!(parse_status_code("HTTP/1.1 OK"), None);
        assert_eq!(parse_status_code("HTTP/1.1 abc OK"), None);
    }

    /// Answer one request and close, the way a probe sees a real reply.
    fn serve_one_response(response: &'static str) -> u16 {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        std::thread::spawn(move || {
            if let Ok((mut socket, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf);
                let _ = socket.write_all(response.as_bytes());
            }
        });
        port
    }

    #[test]
    fn a_ready_response_is_a_successful_probe() {
        let port = serve_one_response("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        assert_eq!(probe(port).expect("probe reads the status line"), 200);
    }

    #[test]
    fn a_not_ready_response_is_reported_with_its_status_code() {
        let port =
            serve_one_response("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(probe(port).expect("probe reads the status line"), 503);
    }

    #[test]
    fn a_port_with_nothing_listening_is_an_error_not_a_status() {
        // Bind and drop, so the port is free but nothing answers on it.
        let port = {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind loopback");
            listener.local_addr().expect("local addr").port()
        };
        assert!(probe(port).is_err());
    }

    #[test]
    fn the_probe_path_is_an_infrastructure_route() {
        // Keeps the probe out of the business-request series and catches a typo
        // in a path the server would answer with 404.
        assert!(
            cog_core::contract::observability::is_infra_endpoint(PROBE_PATH),
            "{PROBE_PATH} is not a recognized probe route"
        );
    }
}
