//! QUIC / HTTP3 transport layer.
//!
//! Exposes two public functions:
//!
//! * [`serve_quic`] — accepts HTTP/3 connections and routes them through the
//!   main Axum `Router` (same handlers as the TCP listener).
//!
//! * [`serve_quic_mgmt`] — a dedicated management endpoint that routes through
//!   a *separate* router containing only status / admin handlers.  This is the
//!   transport for the management UI.
//!
//! # TLS
//! QUIC mandates TLS 1.3.  In development / self-hosted deployments the
//! module generates a **self-signed certificate** at startup using `rcgen`.
//! For production supply real credentials via `QUIC_CERT_PEM` / `QUIC_KEY_PEM`.
//!
//! # Alt-Svc
//! `main.rs` injects an `Alt-Svc: h3=":<port>"` header on TCP responses so
//! HTTP/3-capable clients can discover and upgrade automatically.
//!
//! # Why a separate listener?
//! QUIC runs over UDP; HTTP/1.1 + HTTP/2 run over TCP.  The OS keeps UDP and
//! TCP port namespaces separate, so the QUIC endpoint can share the same port
//! number without conflict.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{body::Body, http, Router};
use bytes::{Buf, Bytes, BytesMut};
use h3_quinn::quinn;
use http_body_util::BodyExt;
use quinn::{crypto::rustls::QuicServerConfig, Endpoint, ServerConfig};
use rustls::{
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    ServerConfig as RustlsServerConfig,
};
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::status::ServerMetrics;

// ══════════════════════════════════════════════════════════════════════════════
// Public entry points
// ══════════════════════════════════════════════════════════════════════════════

/// Accept HTTP/3 connections and route them through the full Axum router.
pub async fn serve_quic(
    addr: SocketAddr,
    router: Router,
    metrics: Arc<ServerMetrics>,
) -> Result<()> {
    let endpoint = build_endpoint(addr)?;
    info!("QUIC listener ready on {}", endpoint.local_addr()?);
    accept_loop(endpoint, Arc::new(router), metrics).await
}

/// Accept HTTP/3 connections on the dedicated management port.
pub async fn serve_quic_mgmt(
    addr: SocketAddr,
    mgmt_router: Router,
    metrics: Arc<ServerMetrics>,
) -> Result<()> {
    let endpoint = build_endpoint(addr)?;
    info!("QUIC management listener ready on {}", endpoint.local_addr()?);
    accept_loop(endpoint, Arc::new(mgmt_router), metrics).await
}

// ══════════════════════════════════════════════════════════════════════════════
// Accept loop (shared by both entry points)
// ══════════════════════════════════════════════════════════════════════════════

async fn accept_loop(
    endpoint: Endpoint,
    router: Arc<Router>,
    metrics: Arc<ServerMetrics>,
) -> Result<()> {
    while let Some(incoming) = endpoint.accept().await {
        let router = router.clone();
        let metrics = metrics.clone();

        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    metrics.quic_open();
                    let result = handle_quic_connection(conn, router).await;
                    metrics.quic_close();

                    if let Err(e) = result {
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

async fn handle_quic_connection(conn: quinn::Connection, router: Arc<Router>) -> Result<()> {
    let remote = conn.remote_address();
    debug!("QUIC connection from {remote}");

    let mut h3_conn: h3::server::Connection<h3_quinn::Connection, Bytes> =
        h3::server::Connection::new(h3_quinn::Connection::new(conn))
            .await
            .context("HTTP/3 connection setup failed")?;

    loop {
        match h3_conn.accept().await {
            Ok(Some(resolver)) => {
                let router = (*router).clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_h3_request(router, resolver).await {
                        debug!("HTTP/3 request error from {remote}: {e}");
                    }
                });
            }
            Ok(None) => break, // connection closed cleanly
            Err(e) => {
                // Distinguish graceful close from real errors by inspecting
                // the debug representation (h3 error types are non-exhaustive).
                let dbg = format!("{e:?}");
                if dbg.contains("NO_ERROR")
                    || dbg.contains("ApplicationClose: 0x0")
                    || dbg.contains("ConnectionClosed")
                {
                    debug!("HTTP/3 connection closed gracefully from {remote}");
                } else {
                    warn!("HTTP/3 connection error from {remote}: {e}");
                }
                break;
            }
        }
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Per-request adapter: h3 → Axum 0.7
// ══════════════════════════════════════════════════════════════════════════════

async fn handle_h3_request(
    router: Router,
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
) -> Result<()> {
    // Resolve the request headers from the QUIC stream.
    let (req_head, mut stream) = resolver
        .resolve_request()
        .await
        .context("Failed to resolve HTTP/3 request headers")?;

    // Collect the request body.
    let mut buf = BytesMut::new();
    loop {
        match stream.recv_data().await {
            Ok(Some(mut chunk)) => {
                buf.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
            }
            Ok(None) => break,
            Err(e) => {
                // Send 400 and close stream on body read error.
                let mut err_resp = http::Response::new(());
                *err_resp.status_mut() = http::StatusCode::BAD_REQUEST;
                let _ = stream.send_response(err_resp).await;
                let _ = stream.finish().await;
                return Err(anyhow::anyhow!("Failed to read request body: {e}"));
            }
        }
    }

    // Build the Axum-compatible request.
    let (parts, _) = req_head.into_parts();
    let axum_req = http::Request::from_parts(parts, Body::from(buf.freeze()));

    // Route through Axum. The infallible error type means unwrap is safe here.
    let axum_resp: http::Response<Body> = router
        .oneshot(axum_req)
        .await
        .context("Axum router error")?;

    // Send the response head over the H3 stream.
    let (resp_parts, resp_body) = axum_resp.into_parts();
    stream
        .send_response(http::Response::from_parts(resp_parts, ()))
        .await
        .context("Failed to send HTTP/3 response head")?;

    // Stream the response body frame by frame.
    let mut pinned = std::pin::pin!(resp_body);
    while let Some(frame_result) = <Body as BodyExt>::frame(&mut pinned).await {
        let frame = frame_result.context("Response body error")?;
        if let Ok(data) = frame.into_data() {
            if !data.is_empty() {
                stream
                    .send_data(data)
                    .await
                    .context("Failed to send HTTP/3 body chunk")?;
            }
        }
    }

    stream
        .finish()
        .await
        .context("Failed to finish HTTP/3 stream")?;

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Endpoint / TLS construction
// ══════════════════════════════════════════════════════════════════════════════

fn build_endpoint(addr: SocketAddr) -> Result<Endpoint> {
    let (cert_chain, private_key) = load_or_generate_tls()?;
    let server_config = build_quinn_config(cert_chain, private_key)?;
    Endpoint::server(server_config, addr)
        .with_context(|| format!("Failed to bind QUIC endpoint on {addr}"))
}

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
                .context("No PKCS8 private key in QUIC_KEY_PEM")?
                .context("Failed to parse QUIC private key")?;

            Ok((certs, key))
        }
        _ => {
            info!("QUIC_CERT_PEM / QUIC_KEY_PEM not set — generating self-signed certificate");
            generate_self_signed()
        }
    }
}

fn generate_self_signed() -> Result<(Vec<CertificateDer<'static>>, PrivatePkcs8KeyDer<'static>)> {
    let hostname = std::env::var("QUIC_HOSTNAME").unwrap_or_else(|_| "localhost".to_string());

    let cert = rcgen::generate_simple_self_signed([hostname])
        .context("Failed to generate self-signed TLS certificate")?;

    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    Ok((vec![cert_der], key_der))
}

fn build_quinn_config(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
) -> Result<ServerConfig> {
    let mut rustls_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key.into())
        .context("Failed to build rustls ServerConfig")?;

    // QUIC requires ALPN; "h3" identifies HTTP/3.
    rustls_config.alpn_protocols = vec![b"h3".to_vec()];

    let quic_config = QuicServerConfig::try_from(Arc::new(rustls_config))
        .context("Failed to build QuicServerConfig")?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_config));

    // Transport parameters tuned for bulk chunk transfers + low-latency admin:
    //   1 MB stream window  ·  8 MB connection window  ·  30 s idle timeout
    let mut transport = quinn::TransportConfig::default();
    transport
        .stream_receive_window(1_000_000u32.into())
        .receive_window(8_000_000u32.into())
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?));

    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}
