use dataplane_sdk_sqlite::SqliteTransaction;

use crate::tokens::model::Token;

use super::TokenRepo;

#[derive(Clone, Default)]
pub struct SqliteTokenRepo;

impl SqliteTokenRepo {}

#[async_trait::async_trait]
impl TokenRepo for SqliteTokenRepo {
    type Transaction = SqliteTransaction;
    async fn create(&self, tx: &mut SqliteTransaction, token: Token) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO tokens (flow_id, endpoint, token_id, dataset_id)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(token.flow_id)
        .bind(token.endpoint)
        .bind(token.token_id)
        .bind(token.dataset_id)
        .execute(&mut *tx.0)
        .await?;
        Ok(())
    }

    async fn get_by_dataset(
        &self,
        tx: &mut SqliteTransaction,
        dataset_id: &str,
    ) -> anyhow::Result<Vec<Token>> {
        sqlx::query_as::<_, Token>(
            r#"
            SELECT * FROM tokens where dataset_id = ?
            "#,
        )
        .bind(dataset_id)
        .fetch_all(&mut *tx.0)
        .await
        .map(Ok)?
    }

    async fn get_by_dataset_and_token_id(
        &self,
        tx: &mut SqliteTransaction,
        dataset_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        sqlx::query_as::<_, Token>(
            r#"
            SELECT * FROM tokens where dataset_id = ? AND token_id = ?
            "#,
        )
        .bind(dataset_id)
        .bind(token_id)
        .fetch_optional(&mut *tx.0)
        .await
        .map(Ok)?
    }
    async fn delete(&self, tx: &mut SqliteTransaction, flow_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM tokens where flow_id = ?
            "#,
        )
        .bind(flow_id)
        .execute(&mut *tx.0)
        .await?;

        Ok(())
    }
}

impl SqliteTokenRepo {
    pub async fn migrate(&self, tx: &mut SqliteTransaction) -> anyhow::Result<()> {
        let mut migrator = sqlx::migrate!("./migrations");
        migrator.set_ignore_missing(true);
        migrator.run(&mut *tx.0).await?;
        Ok(())
    }
}
