use super::utils::Address;
use tonic_health::pb::health_client::HealthClient as _HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::ServingStatus;

pub struct HealthClient;

impl HealthClient {
    pub async fn check(address: &Address) -> anyhow::Result<()> {
        let channel = super::utils::connect(address).await?;
        let mut client = _HealthClient::new(channel);

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
