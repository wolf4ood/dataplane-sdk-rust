// Allow unwraps for simplicity in this in-memory implementation
#![allow(clippy::unwrap_used)]

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::core::error::DbResult;

use super::tx::{Transaction, TransactionalContext};

#[derive(Clone)]
pub struct MemoryRepo<T>(Arc<RwLock<HashMap<String, T>>>);

impl<T> Default for MemoryRepo<T> {
    fn default() -> Self {
        MemoryRepo(Arc::default())
    }
}

impl<T: Clone> MemoryRepo<T> {
    pub async fn create(&self, id: &str, entity: &T) -> DbResult<()> {
        let mut map = self.0.write().unwrap();
        if map.contains_key(id) {
            return Err(crate::core::error::DbError::AlreadyExists(format!(
                "Entity with id {} already exists",
                id
            )));
        }
        map.insert(id.to_string(), entity.clone());
        Ok(())
    }

    pub async fn fetch_by_id(&self, flow_id: &str) -> DbResult<Option<T>> {
        let map = self.0.read().unwrap();
        Ok(map.get(flow_id).cloned())
    }

    pub async fn update(&self, id: &str, entity: &T) -> DbResult<()> {
        let mut map = self.0.write().unwrap();
        if map.contains_key(id) {
            map.insert(id.to_string(), entity.clone());
            Ok(())
        } else {
            Err(crate::core::error::DbError::NotFound(format!(
                "Entity with id {} not found",
                id
            )))
        }
    }

    pub async fn filter<F>(&self, predicate: F) -> DbResult<Vec<T>>
    where
        F: Fn(&T) -> bool,
    {
        let map = self.0.read().unwrap();
        Ok(map.values().filter(|e| predicate(e)).cloned().collect())
    }

    pub async fn delete(&self, id: &str) -> DbResult<()> {
        let mut map = self.0.write().unwrap();
        if map.remove(id).is_some() {
            Ok(())
        } else {
            Err(crate::core::error::DbError::NotFound(format!(
                "Entity with id {} not found",
                id
            )))
        }
    }
}

pub struct MemoryContext;

pub struct MemoryTransaction;

#[async_trait::async_trait]
impl Transaction for MemoryTransaction {
    async fn commit(self) -> DbResult<()> {
        Ok(())
    }

    async fn rollback(self) -> DbResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl TransactionalContext for MemoryContext {
    type Transaction = MemoryTransaction;

    async fn begin(&self) -> DbResult<Self::Transaction> {
        Ok(MemoryTransaction)
    }
}
