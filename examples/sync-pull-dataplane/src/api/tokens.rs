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

use example_common::util::launch_server;
use std::{net::SocketAddr, str::FromStr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use bon::Builder;
use dataplane_sdk::core::db::tx::TransactionalContext;
use dataplane_sdk::sdk::DataPlaneSdk;
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;

use crate::{api::Context, tokens::manager::TokenManager};

use super::ApiError;

pub async fn start_token_api<T>(
    port: u16,
    tokens: Arc<TokenManager<T>>,
    sdk: DataPlaneSdk<T>,
    barrier: Arc<Barrier>,
) where
    T: TransactionalContext + 'static,
    T::Transaction: Send,
{
    let addr = SocketAddr::from_str(&format!("0.0.0.0:{port}")).unwrap();

    launch_server(
        "Token API",
        token_router(),
        Context::builder().tokens(tokens).sdk(sdk).build(),
        addr,
        barrier,
    )
    .await;
}

fn token_router<T>() -> Router<Context<T>>
where
    T: TransactionalContext + 'static,
    T::Transaction: Send,
{
    Router::new().route("/tokens/{dataset_id}", get(get_tokens))
}

async fn get_tokens<T>(
    Path(dataset_id): Path<String>,
    State(ctx): State<Context<T>>,
) -> Result<Json<Vec<TokenResponse>>, ApiError>
where
    T: TransactionalContext + 'static,
    T::Transaction: Send,
{
    let mut tx = ctx
        .sdk
        .ctx()
        .begin()
        .await
        .map_err(|e| ApiError::Generic(e.into()))?;

    let tokens = ctx
        .tokens
        .repo()
        .get_by_dataset(&mut tx, &dataset_id)
        .await
        .map_err(ApiError::Generic)?;

    Ok(Json(
        tokens
            .into_iter()
            .map(|token| {
                TokenResponse::builder()
                    .dataset_id(dataset_id.clone())
                    .token_id(token.token_id)
                    .endpoint(token.endpoint)
                    .build()
            })
            .collect(),
    ))
}

#[derive(Serialize, Deserialize, Builder, Debug)]
pub struct TokenResponse {
    pub dataset_id: String,
    pub token_id: String,
    pub endpoint: String,
}
