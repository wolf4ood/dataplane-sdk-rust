//  Copyright (c) 2026 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Contributors:
//         Metaform Systems, Inc. - initial API and implementation
//

use std::net::TcpStream;

use axum::Router;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

pub async fn wait_for_server(socket: std::net::SocketAddr) {
    for _ in 0..10 {
        if TcpStream::connect_timeout(&socket, std::time::Duration::from_millis(25)).is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

pub async fn launch_server<S: Clone + Send + Sync + 'static>(
    tag: &str,
    router: Router<S>,
    state: S,
    addr: std::net::SocketAddr,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) {
    let service_name = tag.to_string();
    tokio::task::spawn(async move {
        let app = router
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .with_state(state);

        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            tracing::debug!(
                "{service_name} listening on {}",
                listener.local_addr().expect("failed to get local address")
            );
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await;

            tracing::debug!("{service_name} server has been shut down");
        } else {
            tracing::error!("{service_name} failed to bind to {addr}");
        }

        barrier.wait().await;
    });

    wait_for_server(addr).await;
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
