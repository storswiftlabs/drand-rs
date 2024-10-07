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
        let res = HomeResponse {
            status: ("").into(),
            metadata: None,
        };

        Ok(Response::new(res))
    }
}
