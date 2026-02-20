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

use example_common::controlplane::{ControlPlaneSimulator, DataPlaneRequest};
use sync_pull_dataplane::{
    app_client::AppClient,
    config::{ScenarioConfig, load_config},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(env_filter())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = load_config::<ScenarioConfig>()?;

    let cp = ControlPlaneSimulator::builder()
        .consumer(cfg.consumer)
        .provider(cfg.provider)
        .build();

    let dataset_id = Uuid::new_v4().to_string();
    let process_id = Uuid::new_v4().to_string();

    cp.consumer_prepare(
        DataPlaneRequest::builder()
            .dataset_id(dataset_id.clone())
            .process_id(process_id.clone())
            .agreement_id("example-agreement")
            .build(),
    )
    .await?;

    let data_address = cp
        .provider_start(
            DataPlaneRequest::builder()
                .dataset_id(dataset_id.clone())
                .process_id(process_id.clone())
                .agreement_id("example-agreement")
                .build(),
        )
        .await?;

    cp.consumer_started(&process_id, data_address).await?;

    let client = AppClient::builder()
        .base_url("http://localhost:8792")
        .build();

    let data = client.get_data(&dataset_id).await?;

    tracing::info!("Received data: {:?}", data);

    Ok(())
}

fn env_filter() -> EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into())
}
