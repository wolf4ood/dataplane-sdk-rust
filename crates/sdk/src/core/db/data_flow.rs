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

use crate::core::{error::DbResult, model::data_flow::DataFlow};
pub mod memory;

#[cfg(test)]
use crate::core::db::tx::MockTransaction;

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock(type Transaction = MockTransaction;))]
pub trait DataFlowRepo: Send + Sync {
    type Transaction;

    async fn create(&self, tx: &mut Self::Transaction, flow: &DataFlow) -> DbResult<()>;

    async fn fetch_by_id(
        &self,
        tx: &mut Self::Transaction,
        flow_id: &str,
    ) -> DbResult<Option<DataFlow>>;

    async fn update(&self, tx: &mut Self::Transaction, flow: &DataFlow) -> DbResult<()>;

    async fn delete(&self, tx: &mut Self::Transaction, flow_id: &str) -> DbResult<()>;
}
