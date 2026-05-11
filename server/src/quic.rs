//! QUIC / HTTP3 transport layer.
//!
//! This module starts a UDP endpoint (via Quinn) that accepts HTTP/3
//! connections.  Every incoming request is routed through the exact same
//! Axum `Router` that handles the existing TCP/HTTP1.1+HTTP2 traffic, so no
//! handler code needs to be duplicated.
//!
//! # TLS
//! QUIC mandates TLS 1.3.  In development / self-hosted deployments the
//! module generates a **self-signed certificate** at startup using `rcgen`.
//! For production you should supply a real cert via the `QUIC_CERT_PEM` /
//! `QUIC_KEY_PEM` environment variables (PEM files).
//!
//! # Alt-Svc
//! `main.rs` adds an `Alt-Svc` response header on the HTTP/1.1 + HTTP/2
//! listener so that browsers and compatible clients can discover and upgrade
//! to HTTP/3 automatically.
//!
//! # Why a separate listener?
//! QUIC runs over UDP while HTTP/1.1 and HTTP/2 run over TCP.  They cannot
//! share a single socket, so the QUIC endpoint binds its own UDP port
//! (default: same port number as the TCP listener, because the OS keeps UDP
//! and TCP port namespaces separate).

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::Router;
use h3::server::RequestStream;
use h3_quinn::quinn;
use quinn::{crypto::rustls::QuicServerConfig, Endpoint, ServerConfig};
use rustls::{
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    ServerConfig as RustlsServerConfig,
};
use tracing::{debug, error, info, warn};

// ══════════════════════════════════════════════════════════════════════════════
// Public entry point
// ══════════════════════════════════════════════════════════════════════════════

/// Bind a QUIC endpoint and accept HTTP/3 connections indefinitely.
///
/// Call this from `main` with `tokio::spawn` so it runs alongside the
/// existing TCP listener.
///
/// ```ignore
/// tokio::spawn(serve_quic(addr, app.clone()));
/// ```
pub async fn serve_quic(addr: SocketAddr, router: Router) -> Result<()> {
    let (cert_chain, private_key) = load_or_generate_tls()?;

    let server_config = build_quinn_config(cert_chain, private_key)?;
    let endpoint = Endpoint::server(server_config, addr)
        .with_context(|| format!("Failed to bind QUIC endpoint on {addr}"))?;

    info!("QUIC/HTTP3 listener ready on {}", endpoint.local_addr()?);

    // Wrap the router in an `Arc` so it can be cheaply cloned into each
    // connection handler task without re-allocating.
    let router = Arc::new(router);

    while let Some(incoming) = endpoint.accept().await {
        let router = router.clone();

        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_quic_connection(conn, router).await {
                        // Connection-level errors are expected (client disconnects,
                        // resets, idle timeouts) – log at debug, not error.
                        debug!("QUIC connection closed: {e}");
                    }
                }
                Err(e) => warn!("QUIC handshake failed: {e}"),
            }
        });
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Per-connection handler
// ══════════════════════════════════════════════════════════════════════════════

async fn handle_quic_connection(
    conn: quinn::Connection,
    router: Arc<Router>,
) -> Result<()> {
    let remote = conn.remote_address();
    debug!("QUIC connection from {remote}");

    let h3_conn = h3::server::Connection::new(h3_quinn::Connection::new(conn))
        .await
        .context("HTTP/3 connection setup failed")?;

    handle_h3_connection(h3_conn, router, remote).await
}

async fn handle_h3_connection(
    mut conn: h3::server::Connection<h3_quinn::Connection, bytes::Bytes>,
    router: Arc<Router>,
    remote: SocketAddr,
) -> Result<()> {
    loop {
        match conn.accept().await {
            Ok(Some((req, stream))) => {
                let router = router.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_h3_request(req, stream, router, remote).await {
                        debug!("HTTP/3 request error from {remote}: {e}");
                    }
                });
            }
            // Connection closed cleanly.
            Ok(None) => break,
            Err(e) => {
                // Protocol errors on the connection itself — log and close.
                use h3::error::ErrorLevel;
                match e.get_error_level() {
                    ErrorLevel::ConnectionError => {
                        warn!("HTTP/3 connection error from {remote}: {e}");
                        break;
                    }
                    ErrorLevel::StreamError => {
                        // Stream error: skip this stream but keep the connection.
                        debug!("HTTP/3 stream error from {remote}: {e}");
                    }
                }
            }
        }
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Per-request adapter: h3 → Axum
// ══════════════════════════════════════════════════════════════════════════════

async fn handle_h3_request(
    req: http::Request<()>,
    mut stream: RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
    router: Arc<Router>,
    remote: SocketAddr,
) -> Result<()> {
    use axum::body::Body;
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    // Collect the request body from the QUIC stream into an in-memory buffer.
    // For chunk uploads this stays within the CDC max-size (4 MB), so OOM is
    // not a concern here.  Very large bodies (file uploads) should still go
    // through the TCP multipart endpoint.
    let (parts, _) = req.into_parts();
    let mut body_bytes: Vec<Bytes> = Vec::new();
    while let Some(data) = stream.recv_data().await? {
        body_bytes.push(data.into());
    }

    // Reconstruct the request with a proper `Body`.
    let body = Body::from(body_bytes.concat());
    let axum_req = http::Request::from_parts(parts, body);

    // Serve through the same Axum router as the TCP path.
    let response = router
        .clone()
        .oneshot(axum_req)
        .await
        .context("Axum router error")?;

    // Write the HTTP/3 response head.
    let (resp_parts, resp_body) = response.into_parts();
    stream
        .send_response(http::Response::from_parts(resp_parts, ()))
        .await
        .context("Failed to send HTTP/3 response head")?;

    // Stream the response body.
    let mut body = resp_body;
    while let Some(frame) = body.frame().await {
        if let Ok(frame) = frame {
            if let Ok(data) = frame.into_data() {
                stream.send_data(data).await.context("Failed to send HTTP/3 body chunk")?;
            }
        }
    }

    stream.finish().await.context("Failed to finish HTTP/3 stream")?;

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// TLS helpers
// ══════════════════════════════════════════════════════════════════════════════

/// Load TLS credentials from environment variables, or generate a self-signed
/// certificate if none are provided.
///
/// Environment variables:
/// - `QUIC_CERT_PEM`: path to a PEM file containing the certificate chain.
/// - `QUIC_KEY_PEM`:  path to a PEM file containing the private key.
fn load_or_generate_tls() -> Result<(Vec<CertificateDer<'static>>, PrivatePkcs8KeyDer<'static>)> {
    let cert_path = std::env::var("QUIC_CERT_PEM").ok();
    let key_path = std::env::var("QUIC_KEY_PEM").ok();

    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => {
            info!("Loading QUIC TLS certificate from {cert_path}");
            let cert_pem = std::fs::read(&cert_path)
                .with_context(|| format!("Failed to read QUIC cert: {cert_path}"))?;
            let key_pem = std::fs::read(&key_path)
                .with_context(|| format!("Failed to read QUIC key: {key_path}"))?;

            let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .context("Failed to parse QUIC certificate PEM")?;

            let key = rustls_pemfile::pkcs8_private_keys(&mut key_pem.as_slice())
                .next()
                .context("No PKCS8 private key found in QUIC_KEY_PEM")?
                .context("Failed to parse QUIC private key")?;

            Ok((certs, key))
        }
        _ => {
            info!("QUIC_CERT_PEM / QUIC_KEY_PEM not set – generating self-signed certificate");
            generate_self_signed()
        }
    }
}

/// Generate an ephemeral self-signed TLS certificate (ECDSA P-256, 90-day
/// validity).  Suitable for development and private deployments.
fn generate_self_signed() -> Result<(Vec<CertificateDer<'static>>, PrivatePkcs8KeyDer<'static>)> {
    let hostname = std::env::var("QUIC_HOSTNAME")
        .unwrap_or_else(|_| "localhost".to_string());

    let cert = rcgen::generate_simple_self_signed([hostname])
        .context("Failed to generate self-signed TLS certificate")?;

    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    Ok((vec![cert_der], key_der))
}

/// Build a Quinn `ServerConfig` from a TLS certificate and private key.
fn build_quinn_config(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
) -> Result<ServerConfig> {
    let mut rustls_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key.into())
        .context("Failed to build rustls ServerConfig")?;

    // QUIC requires ALPN.  "h3" identifies HTTP/3.
    rustls_config.alpn_protocols = vec![b"h3".to_vec()];

    let quic_config =
        QuicServerConfig::try_from(Arc::new(rustls_config)).context("Failed to build QuicServerConfig")?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_config));

    // Transport parameters tuned for bulk chunk transfers:
    //   - 1 MB initial stream receive window  (per stream)
    //   - 8 MB initial connection receive window
    //   - 30 s max idle timeout
    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(1_000_000u32.into())
        .receive_window(8_000_000u32.into())
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?));

    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}
