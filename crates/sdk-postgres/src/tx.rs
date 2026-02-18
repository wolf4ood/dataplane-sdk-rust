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

use dataplane_sdk::core::{
    db::tx::{Transaction, TransactionalContext},
    error::{DbError, DbResult},
};
use sqlx::{PgPool, Postgres};

pub struct PgContext(PgPool);

impl PgContext {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(database_url).await?;
        Ok(PgContext(pool))
    }
}

#[async_trait::async_trait]
impl TransactionalContext for PgContext {
    type Transaction = PgTransaction;

    async fn begin(&self) -> DbResult<Self::Transaction> {
        let transaction = self
            .0
            .begin()
            .await
            .map_err(|err| DbError::Generic(Box::new(err)))?;

        Ok(PgTransaction(transaction))
    }
}

pub struct PgTransaction(pub sqlx::Transaction<'static, Postgres>);

#[async_trait::async_trait]
impl Transaction for PgTransaction {
    async fn commit(self) -> DbResult<()> {
        self.0
            .commit()
            .await
            .map_err(|err| DbError::Generic(Box::new(err)))
    }

    async fn rollback(self) -> DbResult<()> {
        self.0
            .rollback()
            .await
            .map_err(|err| DbError::Generic(Box::new(err)))
    }
}
