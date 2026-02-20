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

use crate::api::{public::start_public_api, tokens::start_token_api};
use crate::config::DataPlaneConfig;
use crate::handler::TokenHandler;
use crate::tokens::manager::TokenManager;
use crate::tokens::repo::TokenRepo;
use crate::tokens::repo::postgres::PgTokenRepo;
use crate::tokens::repo::sqlite::SqliteTokenRepo;
use dataplane_sdk::core::db::data_flow::DataFlowRepo;
use dataplane_sdk::core::db::tx::{Transaction, TransactionalContext};
use dataplane_sdk::sdk::DataPlaneSdk;
use dataplane_sdk_postgres::{PgContext, PgDataFlowRepo};
use dataplane_sdk_sqlite::{SqliteContext, SqliteDataFlowRepo};
use example_common::signaling::start_signaling;
use tokio::sync::Barrier;

pub async fn start_dataplane(cfg: DataPlaneConfig) -> anyhow::Result<()> {
    match &cfg.db {
        crate::config::Db::Sqlite { url } => {
            let (ctx, repo, token_repo) = setup_sqlite(url).await?;
            internal_launch(&cfg, ctx, repo, token_repo).await
        }
        crate::config::Db::Postgres { url } => {
            let (ctx, repo, token_repo) = setup_pg(url).await?;
            internal_launch(&cfg, ctx, repo, token_repo).await
        }
    }
}

async fn setup_sqlite(
    url: &str,
) -> anyhow::Result<(SqliteContext, SqliteDataFlowRepo, SqliteTokenRepo)> {
    let ctx = SqliteContext::connect(url).await?;

    let mut tx = ctx.begin().await?;
    let repo = SqliteDataFlowRepo;
    let token_repo = SqliteTokenRepo;

    repo.migrate(&mut tx).await?;
    token_repo.migrate(&mut tx).await?;

    tx.commit().await?;

    Ok((ctx, repo, token_repo))
}

async fn setup_pg(url: &str) -> anyhow::Result<(PgContext, PgDataFlowRepo, PgTokenRepo)> {
    let ctx = PgContext::connect(url).await?;

    let mut tx = ctx.begin().await?;
    let repo = PgDataFlowRepo;
    let token_repo = PgTokenRepo;

    repo.migrate(&mut tx).await?;
    token_repo.migrate(&mut tx).await?;

    tx.commit().await?;

    Ok((ctx, repo, token_repo))
}

async fn internal_launch<C, R, T>(
    cfg: &DataPlaneConfig,
    ctx: C,
    flows: R,
    tokens: T,
) -> anyhow::Result<()>
where
    C: TransactionalContext + 'static,
    C::Transaction: Send,
    R: DataFlowRepo<Transaction = C::Transaction> + 'static,
    T: TokenRepo<Transaction = C::Transaction> + 'static,
{
    let token_manager = Arc::new(create_token_manager(cfg, tokens).await?);
    let handler = TokenHandler::new(token_manager.clone());

    let sdk = sdk(ctx, flows, handler).await;

    let barrier = Arc::new(Barrier::new(4));

    start_signaling(cfg.signaling.port, sdk.clone(), barrier.clone()).await;

    start_public_api(
        cfg.public_api.port,
        token_manager.clone(),
        sdk.clone(),
        barrier.clone(),
    )
    .await;

    start_token_api(
        cfg.token_api.port,
        token_manager.clone(),
        sdk,
        barrier.clone(),
    )
    .await;

    tracing::info!("DataPlane is ready");
    barrier.wait().await;
    Ok(())
}

async fn sdk<C, R>(ctx: C, repo: R, handler: TokenHandler<C>) -> DataPlaneSdk<C>
where
    C: TransactionalContext + 'static,
    C::Transaction: Send,
    R: DataFlowRepo<Transaction = C::Transaction> + 'static,
{
    DataPlaneSdk::builder(ctx)
        .with_repo(repo)
        .with_handler(handler)
        .build()
        .unwrap()
}

async fn create_token_manager<
    T: TransactionalContext,
    R: TokenRepo<Transaction = T::Transaction> + 'static,
>(
    cfg: &crate::config::DataPlaneConfig,
    repo: R,
) -> anyhow::Result<TokenManager<T>> {
    let public_api = cfg
        .public_api
        .api_url
        .clone()
        .unwrap_or_else(|| format!("http://localhost:{}/api/v1/public", cfg.public_api.port));

    Ok(TokenManager::builder()
        .url(public_api)
        .repo(Box::new(repo))
        .build())
}
