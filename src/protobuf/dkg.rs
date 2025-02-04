// This file is @generated by prost-build.
/// Packet is a wrapper around the three different types of DKG messages
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Packet {
    #[prost(message, optional, tag = "4")]
    pub metadata: ::core::option::Option<super::drand::Metadata>,
    #[prost(oneof = "packet::Bundle", tags = "1, 2, 3")]
    pub bundle: ::core::option::Option<packet::Bundle>,
}
/// Nested message and enum types in `Packet`.
pub mod packet {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Bundle {
        #[prost(message, tag = "1")]
        Deal(super::DealBundle),
        #[prost(message, tag = "2")]
        Response(super::ResponseBundle),
        #[prost(message, tag = "3")]
        Justification(super::JustificationBundle),
    }
}
/// DealBundle is a packet issued by a dealer that contains each individual
/// deals, as well as the coefficients of the public polynomial he used.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DealBundle {
    /// Index of the dealer that issues these deals
    #[prost(uint32, tag = "1")]
    pub dealer_index: u32,
    /// Coefficients of the public polynomial that is created from the
    /// private polynomial from which the shares are derived.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub commits: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// list of deals for each individual share holders.
    #[prost(message, repeated, tag = "3")]
    pub deals: ::prost::alloc::vec::Vec<Deal>,
    /// session identifier of the protocol run
    #[prost(bytes = "vec", tag = "4")]
    pub session_id: ::prost::alloc::vec::Vec<u8>,
    /// signature over the hash of the deal
    #[prost(bytes = "vec", tag = "5")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
/// Deal contains a share for a participant.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Deal {
    #[prost(uint32, tag = "1")]
    pub share_index: u32,
    /// encryption of the share using ECIES
    #[prost(bytes = "vec", tag = "2")]
    pub encrypted_share: ::prost::alloc::vec::Vec<u8>,
}
/// ResponseBundle is a packet issued by a share holder that contains all the
/// responses (complaint and/or success) to broadcast.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseBundle {
    #[prost(uint32, tag = "1")]
    pub share_index: u32,
    #[prost(message, repeated, tag = "2")]
    pub responses: ::prost::alloc::vec::Vec<Response>,
    /// session identifier of the protocol run
    #[prost(bytes = "vec", tag = "3")]
    pub session_id: ::prost::alloc::vec::Vec<u8>,
    /// signature over the hash of the response
    #[prost(bytes = "vec", tag = "4")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
/// Response holds the response that a participant broadcast after having
/// received a deal.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Response {
    /// index of the dealer for which this response is for
    #[prost(uint32, tag = "1")]
    pub dealer_index: u32,
    /// Status represents a complaint if set to false, a success if set to
    /// true.
    #[prost(bool, tag = "2")]
    pub status: bool,
}
/// JustificationBundle is a packet that holds all justifications a dealer must
/// produce
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JustificationBundle {
    #[prost(uint32, tag = "1")]
    pub dealer_index: u32,
    #[prost(message, repeated, tag = "2")]
    pub justifications: ::prost::alloc::vec::Vec<Justification>,
    /// session identifier of the protocol run
    #[prost(bytes = "vec", tag = "3")]
    pub session_id: ::prost::alloc::vec::Vec<u8>,
    /// signature over the hash of the justification
    #[prost(bytes = "vec", tag = "4")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
/// Justification holds the justification from a dealer after a participant
/// issued a complaint response because of a supposedly invalid deal.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Justification {
    /// represents for who share holder this justification is
    #[prost(uint32, tag = "1")]
    pub share_index: u32,
    /// plaintext share so everyone can see it correct
    #[prost(bytes = "vec", tag = "2")]
    pub share: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct EmptyDkgResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DkgCommand {
    #[prost(message, optional, tag = "1")]
    pub metadata: ::core::option::Option<CommandMetadata>,
    #[prost(oneof = "dkg_command::Command", tags = "2, 3, 4, 5, 6, 7, 9")]
    pub command: ::core::option::Option<dkg_command::Command>,
}
/// Nested message and enum types in `DKGCommand`.
pub mod dkg_command {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Command {
        #[prost(message, tag = "2")]
        Initial(super::FirstProposalOptions),
        #[prost(message, tag = "3")]
        Resharing(super::ProposalOptions),
        #[prost(message, tag = "4")]
        Join(super::JoinOptions),
        #[prost(message, tag = "5")]
        Accept(super::AcceptOptions),
        #[prost(message, tag = "6")]
        Reject(super::RejectOptions),
        #[prost(message, tag = "7")]
        Execute(super::ExecutionOptions),
        #[prost(message, tag = "9")]
        Abort(super::AbortOptions),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandMetadata {
    #[prost(string, tag = "1")]
    pub beacon_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GossipPacket {
    #[prost(message, optional, tag = "1")]
    pub metadata: ::core::option::Option<GossipMetadata>,
    #[prost(oneof = "gossip_packet::Packet", tags = "2, 3, 4, 5, 6, 7")]
    pub packet: ::core::option::Option<gossip_packet::Packet>,
}
/// Nested message and enum types in `GossipPacket`.
pub mod gossip_packet {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Packet {
        #[prost(message, tag = "2")]
        Proposal(super::ProposalTerms),
        #[prost(message, tag = "3")]
        Accept(super::AcceptProposal),
        #[prost(message, tag = "4")]
        Reject(super::RejectProposal),
        #[prost(message, tag = "5")]
        Execute(super::StartExecution),
        #[prost(message, tag = "6")]
        Abort(super::AbortDkg),
        #[prost(message, tag = "7")]
        Dkg(super::DkgPacket),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GossipMetadata {
    #[prost(string, tag = "1")]
    pub beacon_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub address: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FirstProposalOptions {
    #[prost(message, optional, tag = "1")]
    pub timeout: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(uint32, tag = "2")]
    pub threshold: u32,
    #[prost(uint32, tag = "3")]
    pub period_seconds: u32,
    #[prost(string, tag = "4")]
    pub scheme: ::prost::alloc::string::String,
    #[prost(uint32, tag = "5")]
    pub catchup_period_seconds: u32,
    #[prost(message, optional, tag = "6")]
    pub genesis_time: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, repeated, tag = "7")]
    pub joining: ::prost::alloc::vec::Vec<Participant>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProposalOptions {
    #[prost(message, optional, tag = "1")]
    pub timeout: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(uint32, tag = "2")]
    pub threshold: u32,
    #[prost(uint32, tag = "3")]
    pub catchup_period_seconds: u32,
    #[prost(message, repeated, tag = "4")]
    pub joining: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "5")]
    pub leaving: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "6")]
    pub remaining: ::prost::alloc::vec::Vec<Participant>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct AbortOptions {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ExecutionOptions {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinOptions {
    #[prost(bytes = "vec", tag = "1")]
    pub group_file: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct AcceptOptions {}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct RejectOptions {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProposalTerms {
    #[prost(string, tag = "1")]
    pub beacon_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub epoch: u32,
    #[prost(message, optional, tag = "3")]
    pub leader: ::core::option::Option<Participant>,
    #[prost(uint32, tag = "4")]
    pub threshold: u32,
    #[prost(message, optional, tag = "5")]
    pub timeout: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(uint32, tag = "6")]
    pub catchup_period_seconds: u32,
    #[prost(uint32, tag = "7")]
    pub beacon_period_seconds: u32,
    #[prost(string, tag = "8")]
    pub scheme_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "9")]
    pub genesis_time: ::core::option::Option<::prost_types::Timestamp>,
    /// joiners require this as they don't have the
    #[prost(bytes = "vec", tag = "10")]
    pub genesis_seed: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "11")]
    pub joining: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "12")]
    pub remaining: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "13")]
    pub leaving: ::prost::alloc::vec::Vec<Participant>,
}
/// this is in sync with the Identity one in common.proto
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Participant {
    #[prost(string, tag = "1")]
    pub address: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// BLS signature over the identity to prove possession of the private key, it
    /// also verify the scheme used
    #[prost(bytes = "vec", tag = "3")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcceptProposal {
    #[prost(message, optional, tag = "1")]
    pub acceptor: ::core::option::Option<Participant>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RejectProposal {
    /// the person rejecting the proposal
    #[prost(message, optional, tag = "1")]
    pub rejector: ::core::option::Option<Participant>,
    /// the reason for rejection, if applicable
    ///
    /// signature over the proposal message that's being accepted
    #[prost(string, tag = "2")]
    pub reason: ::prost::alloc::string::String,
    /// used to authenticate the user
    #[prost(bytes = "vec", tag = "3")]
    pub secret: ::prost::alloc::vec::Vec<u8>,
    /// In resharing cases, previous_group_hash is the hash of the previous group.
    /// It is to make sure the nodes build on top of the correct previous group.
    #[prost(bytes = "vec", tag = "4")]
    pub previous_group_hash: ::prost::alloc::vec::Vec<u8>,
    /// a sha256 hash of the original proposal message
    #[prost(bytes = "vec", tag = "5")]
    pub proposal_hash: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbortDkg {
    #[prost(string, tag = "1")]
    pub reason: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct StartExecution {
    #[prost(message, optional, tag = "1")]
    pub time: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DkgStatusRequest {
    #[prost(string, tag = "1")]
    pub beacon_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DkgStatusResponse {
    #[prost(message, optional, tag = "1")]
    pub complete: ::core::option::Option<DkgEntry>,
    #[prost(message, optional, tag = "2")]
    pub current: ::core::option::Option<DkgEntry>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DkgEntry {
    #[prost(string, tag = "1")]
    pub beacon_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub state: u32,
    #[prost(uint32, tag = "3")]
    pub epoch: u32,
    #[prost(uint32, tag = "4")]
    pub threshold: u32,
    #[prost(message, optional, tag = "5")]
    pub timeout: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag = "6")]
    pub genesis_time: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(bytes = "vec", tag = "7")]
    pub genesis_seed: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "8")]
    pub leader: ::core::option::Option<Participant>,
    #[prost(message, repeated, tag = "9")]
    pub remaining: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "10")]
    pub joining: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "11")]
    pub leaving: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "12")]
    pub acceptors: ::prost::alloc::vec::Vec<Participant>,
    #[prost(message, repeated, tag = "13")]
    pub rejectors: ::prost::alloc::vec::Vec<Participant>,
    #[prost(string, repeated, tag = "14")]
    pub final_group: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// DKGPacket is the packet that nodes send to others nodes as part of the
/// broadcasting protocol.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DkgPacket {
    #[prost(message, optional, tag = "1")]
    pub dkg: ::core::option::Option<Packet>,
}
/// Generated client implementations.
pub mod dkg_control_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct DkgControlClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DkgControlClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DkgControlClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DkgControlClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            DkgControlClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn command(
            &mut self,
            request: impl tonic::IntoRequest<super::DkgCommand>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/dkg.DKGControl/Command");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("dkg.DKGControl", "Command"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn packet(
            &mut self,
            request: impl tonic::IntoRequest<super::GossipPacket>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/dkg.DKGControl/Packet");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("dkg.DKGControl", "Packet"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn dkg_status(
            &mut self,
            request: impl tonic::IntoRequest<super::DkgStatusRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DkgStatusResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/dkg.DKGControl/DKGStatus");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("dkg.DKGControl", "DKGStatus"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn broadcast_dkg(
            &mut self,
            request: impl tonic::IntoRequest<super::DkgPacket>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/dkg.DKGControl/BroadcastDKG",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("dkg.DKGControl", "BroadcastDKG"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod dkg_control_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with DkgControlServer.
    #[async_trait]
    pub trait DkgControl: std::marker::Send + std::marker::Sync + 'static {
        async fn command(
            &self,
            request: tonic::Request<super::DkgCommand>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        >;
        async fn packet(
            &self,
            request: tonic::Request<super::GossipPacket>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        >;
        async fn dkg_status(
            &self,
            request: tonic::Request<super::DkgStatusRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DkgStatusResponse>,
            tonic::Status,
        >;
        async fn broadcast_dkg(
            &self,
            request: tonic::Request<super::DkgPacket>,
        ) -> std::result::Result<
            tonic::Response<super::EmptyDkgResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct DkgControlServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> DkgControlServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DkgControlServer<T>
    where
        T: DkgControl,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/dkg.DKGControl/Command" => {
                    #[allow(non_camel_case_types)]
                    struct CommandSvc<T: DkgControl>(pub Arc<T>);
                    impl<T: DkgControl> tonic::server::UnaryService<super::DkgCommand>
                    for CommandSvc<T> {
                        type Response = super::EmptyDkgResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DkgCommand>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DkgControl>::command(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CommandSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/dkg.DKGControl/Packet" => {
                    #[allow(non_camel_case_types)]
                    struct PacketSvc<T: DkgControl>(pub Arc<T>);
                    impl<T: DkgControl> tonic::server::UnaryService<super::GossipPacket>
                    for PacketSvc<T> {
                        type Response = super::EmptyDkgResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GossipPacket>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DkgControl>::packet(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = PacketSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/dkg.DKGControl/DKGStatus" => {
                    #[allow(non_camel_case_types)]
                    struct DKGStatusSvc<T: DkgControl>(pub Arc<T>);
                    impl<
                        T: DkgControl,
                    > tonic::server::UnaryService<super::DkgStatusRequest>
                    for DKGStatusSvc<T> {
                        type Response = super::DkgStatusResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DkgStatusRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DkgControl>::dkg_status(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DKGStatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/dkg.DKGControl/BroadcastDKG" => {
                    #[allow(non_camel_case_types)]
                    struct BroadcastDKGSvc<T: DkgControl>(pub Arc<T>);
                    impl<T: DkgControl> tonic::server::UnaryService<super::DkgPacket>
                    for BroadcastDKGSvc<T> {
                        type Response = super::EmptyDkgResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DkgPacket>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DkgControl>::broadcast_dkg(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = BroadcastDKGSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for DkgControlServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "dkg.DKGControl";
    impl<T> tonic::server::NamedService for DkgControlServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
