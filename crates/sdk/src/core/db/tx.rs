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

use crate::core::error::DbResult;

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock(type Transaction = MockTransaction;))]
pub trait TransactionalContext: Send + Sync {
    type Transaction: Transaction;

    /// Begin a new transaction.
    async fn begin(&self) -> DbResult<Self::Transaction>;
}

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait Transaction {
    /// Commit the transaction.
    async fn commit(self) -> DbResult<()>;

    /// Rollback the transaction.
    async fn rollback(self) -> DbResult<()>;
}
