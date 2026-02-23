use super::model::Token;

pub mod memory;
pub mod postgres;

#[async_trait::async_trait]
pub trait TokenRepo: Send + Sync {
    type Transaction;
    async fn create(&self, tx: &mut Self::Transaction, token: Token) -> anyhow::Result<()>;
    async fn get_by_dataset_and_token_id(
        &self,
        tx: &mut Self::Transaction,
        dataset_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>>;

    async fn get_by_dataset(
        &self,
        tx: &mut Self::Transaction,
        dataset_id: &str,
    ) -> anyhow::Result<Vec<Token>>;
    async fn delete(&self, tx: &mut Self::Transaction, id: &str) -> anyhow::Result<()>;
}
