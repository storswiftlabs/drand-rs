// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client implementation for healthcheck service.
use super::utils::Address;
use tonic_health::{
    pb::{health_client::HealthClient as HealthClientInner, HealthCheckRequest},
    ServingStatus,
};

pub struct HealthClient;

impl HealthClient {
    pub async fn check(address: &Address) -> anyhow::Result<()> {
        let channel = super::utils::connect(address).await?;
        let mut client = HealthClientInner::new(channel);

        let resp = client
            .check(HealthCheckRequest::default())
            .await?
            .into_inner();

        if resp.status != ServingStatus::Serving as i32 {
            anyhow::bail!("health: {address} not serving")
        }

        Ok(())
    }
}
