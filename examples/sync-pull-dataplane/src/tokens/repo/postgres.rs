use dataplane_sdk_postgres::PgTransaction;

use crate::tokens::model::Token;

use super::TokenRepo;

#[derive(Clone, Default)]
pub struct PgTokenRepo;

impl PgTokenRepo {}

#[async_trait::async_trait]
impl TokenRepo for PgTokenRepo {
    type Transaction = PgTransaction;
    async fn create(&self, tx: &mut PgTransaction, token: Token) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO tokens (flow_id, endpoint, token_id, dataset_id)
            VALUES ($1, $2, $3, $4)
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
        tx: &mut PgTransaction,
        dataset_id: &str,
    ) -> anyhow::Result<Vec<Token>> {
        sqlx::query_as::<_, Token>(
            r#"
            SELECT * FROM tokens where dataset_id = $1
            "#,
        )
        .bind(dataset_id)
        .fetch_all(&mut *tx.0)
        .await
        .map(Ok)?
    }

    async fn get_by_dataset_and_token_id(
        &self,
        tx: &mut PgTransaction,
        dataset_id: &str,
        token_id: &str,
    ) -> anyhow::Result<Option<Token>> {
        sqlx::query_as::<_, Token>(
            r#"
            SELECT * FROM tokens where dataset_id = $1 AND token_id = $2
            "#,
        )
        .bind(dataset_id)
        .bind(token_id)
        .fetch_optional(&mut *tx.0)
        .await
        .map(Ok)?
    }
    async fn delete(&self, tx: &mut PgTransaction, flow_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM tokens where flow_id = $1
            "#,
        )
        .bind(flow_id)
        .execute(&mut *tx.0)
        .await?;

        Ok(())
    }
}

impl PgTokenRepo {
    pub async fn migrate(&self, tx: &mut PgTransaction) -> anyhow::Result<()> {
        let mut migrator = sqlx::migrate!("./migrations");
        migrator.set_ignore_missing(true);
        migrator.run(&mut *tx.0).await?;
        Ok(())
    }
}
