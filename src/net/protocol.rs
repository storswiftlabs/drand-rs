// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::core::daemon::Daemon;
use crate::net::transport::DkgInfo;
use crate::protobuf::drand::IdentityResponse;
//use crate::net::transport::IdentityResponse;
use crate::net::transport;
use crate::net::transport::ProtoConvert;

use crate::net::utils::get_ref_id;
use crate::protobuf::common::Empty;
use crate::protobuf::drand::protocol_server::Protocol;
use crate::protobuf::drand::DkgInfoPacket;
use crate::protobuf::drand::DkgPacket;
use crate::protobuf::drand::IdentityRequest;
use crate::protobuf::drand::PartialBeaconPacket;
use crate::protobuf::drand::SignalDkgPacket;

use anyhow::Result;
use std::sync::Arc;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::trace;

use super::transport::Bundle;

#[derive(Clone)]
pub struct ProtocolHandler(Arc<Daemon>);

impl ProtocolHandler {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        Self(daemon)
    }
}

impl std::ops::Deref for ProtocolHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl Protocol for ProtocolHandler {
    async fn partial_beacon(
        &self,
        req: Request<PartialBeaconPacket>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let mut packet = req.into_inner();
        let metadata = packet.metadata.take();

        let id = get_ref_id(metadata.as_ref());
        self.new_partial(packet, id).await?;

        Ok(Response::new(Empty::default()))
    }

    async fn push_dkg_info(
        &self,
        req: Request<DkgInfoPacket>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let mut packet = req.into_inner();
        let metadata = packet.metadata.take();

        let dkg_info = DkgInfo::from_proto(packet)?;
        let id = get_ref_id(metadata.as_ref());
        self.push_new_group(dkg_info, id).await?;

        Ok(Response::new(Empty::default()))
    }

    async fn broadcast_dkg(
        &self,
        req: Request<DkgPacket>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let mut packet = req.into_inner();
        let metadata = packet.metadata.take();

        let now = tokio::time::Instant::now();
        let bundle = Bundle::from_proto(packet)?;
        let new_now = tokio::time::Instant::now();

        trace!(parent: self.span(), "$$$ {} Bundle::from_proto {:?}",bundle, new_now.checked_duration_since(now).unwrap());
        let id = get_ref_id(metadata.as_ref());
        self.broadcast_dkg_bundle(bundle, id).await?;

        Ok(Response::new(Empty::default()))
    }

    async fn signal_dkg_participant(
        &self,
        _request: Request<SignalDkgPacket>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::failed_precondition("Leader logic is not implemented yet"))
    }

    async fn get_identity(
        &self,
        req: Request<IdentityRequest>,
    ) -> Result<Response<IdentityResponse>, Status> {
        let id = get_ref_id(req.get_ref().metadata.as_ref());
        let responce: transport::IdentityResponse = self.identity_request(id).await?;

        Ok(Response::new(responce.to_proto()))
    }
}
