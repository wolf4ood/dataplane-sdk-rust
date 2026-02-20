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

use crate::api::{public::Dataset, tokens::TokenResponse};
use bon::Builder;

#[derive(Builder)]
#[builder(on(String, into))]
pub struct AppClient {
    base_url: String,
    #[builder(default = reqwest::Client::new())]
    client: reqwest::Client,
}

impl AppClient {
    pub async fn get_data(&self, dataset_id: &str) -> anyhow::Result<Dataset> {
        let url = format!("{}/tokens/{}", self.base_url, dataset_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Failed to get data: {} - {}",
                response.status(),
                response.text().await?
            );
        }

        let data = response.json::<Vec<TokenResponse>>().await?;

        if let Some(token) = data.first() {
            self.client
                .get(format!("{}/datasets/{}", token.endpoint, dataset_id))
                .header("Authorization", format!("Bearer {}", token.token_id))
                .send()
                .await?
                .json::<Dataset>()
                .await
                .map(Ok)?
        } else {
            anyhow::bail!("No tokens received for dataset {}", dataset_id);
        }
    }
}
