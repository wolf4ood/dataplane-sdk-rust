use crate::core::{
    db::memory::{MemoryRepo, MemoryTransaction},
    error::DbResult,
    model::data_flow::DataFlow,
};

use super::DataFlowRepo;

#[derive(Default, Clone)]
pub struct MemoryDataFlowRepo(MemoryRepo<DataFlow>);

#[async_trait::async_trait]
impl DataFlowRepo for MemoryDataFlowRepo {
    type Transaction = MemoryTransaction;
    async fn create(&self, _tx: &mut Self::Transaction, flow: &DataFlow) -> DbResult<()> {
        self.0.create(&flow.id, flow).await
    }

    async fn fetch_by_id(
        &self,
        _tx: &mut Self::Transaction,
        flow_id: &str,
    ) -> DbResult<Option<DataFlow>> {
        self.0.fetch_by_id(flow_id).await
    }

    async fn update(&self, _tx: &mut Self::Transaction, flow: &DataFlow) -> DbResult<()> {
        self.0.update(&flow.id, flow).await
    }

    async fn delete(&self, _tx: &mut Self::Transaction, flow_id: &str) -> DbResult<()> {
        self.0.delete(flow_id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::core::db::memory::MemoryContext;
    use crate::core::db::memory::MemoryTransaction;
    use crate::core::db::test_suite::Tester;
    use crate::core::db::test_suite::generate_data_flow_store_tests;
    use crate::core::db::tx::TransactionalContext;

    use super::MemoryDataFlowRepo;

    pub struct MemoryTester {
        repo: MemoryDataFlowRepo,
        ctx: MemoryContext,
    }

    impl Tester<MemoryDataFlowRepo, MemoryContext> for MemoryTester {
        async fn create() -> Self {
            let ctx = MemoryContext;
            let repo = MemoryDataFlowRepo::default();
            MemoryTester { repo, ctx }
        }

        fn store(&self) -> &MemoryDataFlowRepo {
            &self.repo
        }

        async fn begin(&self) -> MemoryTransaction {
            self.ctx.begin().await.expect("Failed to begin transaction")
        }
    }

    generate_data_flow_store_tests!(MemoryTester);
}
