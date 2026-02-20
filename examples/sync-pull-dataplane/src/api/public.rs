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
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use bon::Builder;
use dataplane_sdk::core::db::tx::TransactionalContext;
use dataplane_sdk::sdk::DataPlaneSdk;
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use uuid::Uuid;

use crate::{api::Context, tokens::manager::TokenManager};

use super::ApiError;

pub async fn start_public_api<T>(
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
        "Public API",
        public_router(),
        Context::builder().tokens(tokens).sdk(sdk).build(),
        addr,
        barrier,
    )
    .await;
}

fn public_router<T>() -> Router<Context<T>>
where
    T: TransactionalContext + 'static,
    T::Transaction: Send,
{
    Router::new().route("/datasets/{dataset_id}", get(get_dataset))
}

async fn get_dataset<T: TransactionalContext>(
    Path(dataset_id): Path<String>,
    State(ctx): State<Context<T>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<Dataset>, ApiError> {
    let mut tx = ctx
        .sdk
        .ctx()
        .begin()
        .await
        .map_err(|e| ApiError::Generic(e.into()))?;

    ctx.tokens
        .repo()
        .get_by_dataset_and_token_id(&mut tx, &dataset_id, auth.token())
        .await
        .map_err(ApiError::Generic)?
        .ok_or(ApiError::Unauthorized)?;

    Ok(Json(
        Dataset::builder()
            .dataset_id(dataset_id)
            .data(Uuid::new_v4().to_string()) // In a real implementation, this would be the actual data endpoint
            .build(),
    ))
}

#[derive(Serialize, Deserialize, Builder, Debug)]
pub struct Dataset {
    pub dataset_id: String,
    pub data: String,
}
