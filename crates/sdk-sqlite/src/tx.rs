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
use sqlx::{Sqlite, SqlitePool};

pub struct SqliteContext(SqlitePool);

impl SqliteContext {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(database_url).await?;
        Ok(SqliteContext(pool))
    }
}

#[async_trait::async_trait]
impl TransactionalContext for SqliteContext {
    type Transaction = SqliteTransaction;

    async fn begin(&self) -> DbResult<Self::Transaction> {
        let transaction = self
            .0
            .begin()
            .await
            .map_err(|err| DbError::Generic(Box::new(err)))?;

        Ok(SqliteTransaction(transaction))
    }
}

pub struct SqliteTransaction(pub sqlx::Transaction<'static, Sqlite>);

#[async_trait::async_trait]
impl Transaction for SqliteTransaction {
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
