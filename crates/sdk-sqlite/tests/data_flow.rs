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

use dataplane_sdk::core::db::{
    test_suite::Tester,
    tx::{Transaction, TransactionalContext},
};
use dataplane_sdk_sqlite::{SqliteContext, SqliteDataFlowRepo, SqliteTransaction};

pub struct SqliteTester {
    repo: SqliteDataFlowRepo,
    ctx: SqliteContext,
}

impl Tester<SqliteDataFlowRepo, SqliteContext> for SqliteTester {
    async fn create() -> Self {
        let ctx = SqliteContext::connect("sqlite::memory:")
            .await
            .expect("Failed to connect to SQLite database");

        let repo = SqliteDataFlowRepo;

        let mut tx = ctx.begin().await.expect("Failed to begin transaction");

        repo.migrate(&mut tx)
            .await
            .expect("Failed to migrate database");

        tx.commit().await.expect("Failed to commit transaction");

        SqliteTester { repo, ctx }
    }

    fn store(&self) -> &SqliteDataFlowRepo {
        &self.repo
    }

    async fn begin(&self) -> SqliteTransaction {
        self.ctx.begin().await.expect("Failed to begin transaction")
    }
}

mod sqlite {
    use super::SqliteTester;
    use dataplane_sdk::core::db::test_suite::Tester;
    use dataplane_sdk::core::db::test_suite::generate_data_flow_store_tests;

    generate_data_flow_store_tests!(SqliteTester);
}
