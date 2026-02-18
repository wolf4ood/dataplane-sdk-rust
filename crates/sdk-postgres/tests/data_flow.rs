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
use dataplane_sdk_postgres::{PgContext, PgDataFlowRepo, PgTransaction};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

pub struct PgTester {
    repo: PgDataFlowRepo,
    ctx: PgContext,
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<Postgres>,
}

impl Tester<PgDataFlowRepo, PgContext> for PgTester {
    async fn create() -> Self {
        let (ctx, container) = setup_postgres_container().await;

        let repo = PgDataFlowRepo;

        let mut tx = ctx.begin().await.expect("Failed to begin transaction");

        repo.migrate(&mut tx)
            .await
            .expect("Failed to migrate database");

        tx.commit().await.expect("Failed to commit transaction");

        PgTester {
            repo,
            ctx,
            container,
        }
    }

    fn store(&self) -> &PgDataFlowRepo {
        &self.repo
    }

    async fn begin(&self) -> PgTransaction {
        self.ctx.begin().await.expect("Failed to begin transaction")
    }
}

pub async fn setup_postgres_container() -> (PgContext, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.unwrap();

    let connection_string = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        container.get_host_port_ipv4(5432).await.unwrap()
    );

    // Wait for PostgreSQL to be ready with timeout
    let ctx = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
        loop {
            match PgContext::connect(&connection_string).await {
                Ok(pool) => break pool,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("PostgreSQL launch failed"));

    (ctx, container)
}

mod postgres {
    use super::PgTester;
    use dataplane_sdk::core::db::test_suite::Tester;
    use dataplane_sdk::core::db::test_suite::generate_data_flow_store_tests;

    generate_data_flow_store_tests!(PgTester);
}
