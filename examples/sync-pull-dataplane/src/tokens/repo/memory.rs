use dataplane_sdk::core::db::memory::{MemoryRepo, MemoryTransaction};

use crate::tokens::model::Token;

use super::TokenRepo;

#[derive(Clone, Default)]
pub struct MemoryTokenRepo(MemoryRepo<Token>);

impl MemoryTokenRepo {}

#[async_trait::async_trait]
impl TokenRepo for MemoryTokenRepo {
    type Transaction = MemoryTransaction;
    async fn create(&self, _tx: &mut Self::Transaction, token: Token) -> anyhow::Result<()> {
        self.0.create(&token.token_id, &token).await?;
        Ok(())
    }

    async fn get_by_dataset(
        &self,
        _tx: &mut Self::Transaction,
        dataset_id: &str,
    ) -> anyhow::Result<Vec<Token>> {
        self.0
            .filter(|token| token.dataset_id == dataset_id)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to get tokens by dataset_id {}: {}", dataset_id, e)
            })
    }

    async fn get_by_dataset_and_token_id(
        &self,
        _tx: &mut Self::Transaction,
        dataset_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        Ok(self
            .0
            .filter(|token| token.dataset_id == dataset_id && token.token_id == token_id)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get token by dataset_id {} and token_id {}: {}",
                    dataset_id,
                    token_id,
                    e
                )
            })?
            .into_iter()
            .next())
    }
    async fn delete(&self, _tx: &mut Self::Transaction, flow_id: &str) -> anyhow::Result<()> {
        self.0.delete(flow_id).await?;
        Ok(())
    }
}

impl MemoryTokenRepo {}
