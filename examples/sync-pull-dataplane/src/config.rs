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

use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use config::{Config, Environment, File};
use serde::{Deserialize, de::DeserializeOwned};

#[derive(Deserialize, Clone)]
pub struct ScenarioConfig {
    pub consumer_url: String,
    pub provider_url: String,
    pub consumer_token_url: String,
}

#[derive(Deserialize, Clone)]
pub struct DataPlaneConfig {
    pub public_api: PublicApi,
    pub token_api: TokenApi,
    pub signaling: SignalingConfig,
    pub db: Db,
}

#[derive(Deserialize, Clone)]
pub struct SignalingConfig {
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct PublicApi {
    #[serde(default = "default_public_api_port")]
    pub port: u16,
    #[serde(default = "default_bind")]
    pub bind: IpAddr,
    pub api_url: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct TokenApi {
    #[serde(default = "default_token_api_port")]
    pub port: u16,
    #[serde(default = "default_bind")]
    pub bind: IpAddr,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Db {
    Memory,
    Postgres { url: String },
}

pub fn default_public_api_port() -> u16 {
    8789
}

pub fn default_token_api_port() -> u16 {
    8790
}

pub fn default_bind() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
}

pub fn load_config<T: DeserializeOwned>() -> anyhow::Result<T> {
    let path = std::env::args().nth(1);
    let config_file = std::env::var("CONFIG_FILE")
        .map(PathBuf::from)
        .ok()
        .or_else(|| path.map(PathBuf::from));

    let mut config_buider = Config::builder();
    if let Some(path) = config_file {
        config_buider = config_buider.add_source(File::from(path.clone()));
    }

    config_buider
        .add_source(Environment::with_prefix("app"))
        .build()?
        .try_deserialize()
        .map(Ok)?
}
