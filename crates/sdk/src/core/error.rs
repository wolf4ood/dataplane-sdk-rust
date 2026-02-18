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

pub type DbResult<T> = Result<T, DbError>;
pub type HandlerResult<T> = Result<T, HandlerError>;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Database connection error: {0}")]
    Generic(Box<dyn std::error::Error + Send + Sync>),
    #[error("Not supported: {0}")]
    NotSupported(String),
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Database connection error: {0}")]
    Generic(Box<dyn std::error::Error + Send + Sync>),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
}
