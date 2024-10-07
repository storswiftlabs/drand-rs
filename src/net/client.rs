use super::transport::IdentityResponse;
use crate::key::common::Identity;
use crate::net::transport::ProtoConvert;
use crate::protobuf::common::Metadata;
use crate::protobuf::drand::protocol_client::ProtocolClient as _ProtocolClient;
use crate::protobuf::drand::IdentityRequest;
use crate::protobuf::drand::SignalDkgPacket;

use anyhow::bail;
use anyhow::Result;
use http::uri::PathAndQuery;
use http::uri::Scheme;
use http::uri::Uri;
use tonic::transport::Channel;

pub struct ProtocolClient(_ProtocolClient<Channel>);

impl ProtocolClient {
    pub async fn new(addr: &str, tls: bool) -> Result<Self> {
        let uri = uri_from_addr(addr, tls)?;
        let client = _ProtocolClient::connect(uri).await.map_err(|e| {
            anyhow::anyhow!("protocol connection to [addr:{addr}, tls:{tls}]: {e:?}")
        })?;

        Ok(Self(client))
    }

    pub async fn get_identity(&mut self, beacon_id: &str) -> Result<IdentityResponse> {
        let req = IdentityRequest {
            metadata: Some(Metadata {
                beacon_id: beacon_id.to_string(),
                ..Default::default()
            }),
        };
        let responce = self
            .0
            .get_identity(req)
            .await
            .map_err(|e| anyhow::anyhow!("get_identity: {e:?}"))?
            .into_inner();

        Ok(IdentityResponse::from_proto(responce))
    }

    pub async fn signal_dkg_participant(
        &mut self,
        identity: Identity,
        secret: &[u8],
        beacon_id: &str,
    ) -> Result<()> {
        let req = SignalDkgPacket {
            node: Some(identity.to_proto()),
            secret_proof: secret.to_vec(),
            metadata: Some(Metadata {
                beacon_id: beacon_id.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        if let Err(e) = self.0.signal_dkg_participant(req).await {
            bail!("signal_dkg_participant: {e:?}")
        }

        Ok(())
    }
}

// TODO: this is dublicate, use 'Address' instead
pub fn uri_from_addr(addr: &str, tls: bool) -> anyhow::Result<Uri> {
    let mut uri = addr.parse::<Uri>()?;

    if uri.host().is_some()
        && uri.port().is_some()
        && uri.scheme().is_none()
        && uri.path_and_query().is_none()
    {
        let mut parts = uri.into_parts();
        parts.scheme = if tls {
            Some(Scheme::HTTPS)
        } else {
            Some(Scheme::HTTP)
        };
        parts.path_and_query = Some(PathAndQuery::from_static("/"));
        uri = Uri::from_parts(parts)?;
    } else {
        bail!("expected HOST:PORT, received: {addr}",)
    }
    Ok(uri)
}
