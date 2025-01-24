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

use crate::protobuf::drand::public_server::Public;
use crate::protobuf::drand::HomeRequest;
use crate::protobuf::drand::HomeResponse;

use tonic::Request;
use tonic::Response;
use tonic::Status;

pub struct PublicHandler;

#[tonic::async_trait]
impl Public for PublicHandler {
    async fn home(&self, _request: Request<HomeRequest>) -> Result<Response<HomeResponse>, Status> {
        let res = HomeResponse { status: ("").into(), metadata: None };

        Ok(Response::new(res))
    }
}
