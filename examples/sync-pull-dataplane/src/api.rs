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

use std::sync::Arc;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bon::Builder;
use dataplane_sdk::{core::db::tx::TransactionalContext, sdk::DataPlaneSdk};
use serde_json::json;
use tracing::error;

pub mod public;
pub mod tokens;

use crate::tokens::manager::TokenManager;

#[derive(Builder)]
pub struct Context<T: TransactionalContext> {
    tokens: Arc<TokenManager<T>>,
    sdk: DataPlaneSdk<T>,
}

impl<T> Clone for Context<T>
where
    T: TransactionalContext,
{
    fn clone(&self) -> Self {
        Self {
            tokens: self.tokens.clone(),
            sdk: self.sdk.clone(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("Internal server error: {0}")]
    Generic(anyhow::Error),
    #[error("Unauthorized")]
    Unauthorized,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::Generic(e) => {
                error!("Internal server error: {:#}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_owned(),
                )
            }
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_owned()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
