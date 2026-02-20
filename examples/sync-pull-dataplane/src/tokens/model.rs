use bon::Builder;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use thiserror::Error;

#[derive(Builder, FromRow, Clone, Debug, PartialEq)]
pub struct Token {
    #[builder(into)]
    pub flow_id: String,
    #[builder(into)]
    pub token_id: String,
    #[builder(into)]
    pub dataset_id: String,
    #[builder(into)]
    pub endpoint: String,
}

#[derive(Error, Debug)]
pub enum TokenError {
    #[error("Generic error: {0}")]
    Generic(anyhow::Error),
}

#[derive(Deserialize)]
pub struct TokenRequest {
    pub refresh_token: String,
    pub client_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: String,
}
