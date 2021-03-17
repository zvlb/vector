use crate::{
    config::{DataType, GenerateConfig, GlobalOptions, Resource, SourceConfig, SourceDescription},
    event::proto,
    internal_events::{
        HTTPBadRequest, HTTPDecompressError, HTTPEventsReceived, VectorEventReceived,
        VectorProtoDecodeError,
    },
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    sources::{util::SocketListenAddr, Source},
    tcp::TcpKeepaliveConfig,
    tls::{MaybeTlsSettings, TlsConfig},
    tracing::Instrument,
    Event, Pipeline,
};

use async_trait::async_trait;
use bytes::{buf::BufExt, Bytes, BytesMut};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use getset::Setters;
use headers::{Authorization, HeaderMapExt};
use hyper::service::make_service_fn;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::{metadata::MetadataValue, transport::Server, Request, Response, Status};
use vector_proto::vector_server::{Vector, VectorServer};
use vector_proto::{EventRequest, EventResponse};
use warp::{
    filters::BoxedFilter,
    http::{HeaderMap, StatusCode},
    path::{FullPath, Tail},
    reject::Rejection,
    Filter,
};

use std::{collections::HashMap, convert::TryFrom, error::Error, fmt, io::Read, net::SocketAddr};

pub mod vector_proto {
    tonic::include_proto!("vector");
}

#[derive(Default, Debug, Clone)]
pub struct VectorSourceService {
    out: Pipeline,
}

#[tonic::async_trait]
impl Vector for VectorSourceService {
    async fn push_events(
        &self,
        request: Request<EventRequest>,
    ) -> Result<Response<EventResponse>, Status> {
        println!("GetEvents = {:?}", request);
        let reply = vector_proto::EventAck {
            message: "nonsense",
        };
        // Get event from request
        // event_from_req(request.body)
        // Then send event to pipeline
        self.out.send() //'send' `Event`
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Setters)]
#[serde(deny_unknown_fields)]
pub struct VectorSourceConfig {
    pub address: SocketAddr,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[set = "pub"]
    tls: Option<TlsConfig>,
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

inventory::submit! {
    SourceDescription::new::<VectorSourceConfig>("vector")
}

impl GenerateConfig for VectorSourceConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:80".parse().unwrap(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            tls: None,
        })
        .unwrap()
    }
}

#[tonic::async_trait]
#[typetag::serde(name = "vector")]
impl SourceConfig for VectorSourceConfig {
    async fn build(
        &self,
        _name: &str,
        _globals: &GlobalOptions,
        shutdown: ShutdownSignal,
        out: Pipeline,
    ) -> crate::Result<Source> {
        let vector = VectorSource;
        Ok(vector
            .run(self.address, "events", true, &self.tls, out, shutdown)
            .map_err(|error| {
                error!(message = "Source future failed.", %error);
            }))
    }

    fn output_type(&self) -> DataType {
        DataType::Any
    }

    fn source_type(&self) -> &'static str {
        "vector"
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }
}

#[derive(Debug, Clone)]
struct VectorSource;

impl VectorSource {
    async fn run<O>(
        self,
        address: SocketAddr,
        path: &str,
        strict_path: bool,
        tls: &Option<TlsConfig>,
        out: O,
        shutdown: ShutdownSignal,
    ) -> crate::Result<()>
    where
        O: Sink<Event> + Send + 'static + Unpin,
        <O as Sink<Event>>::Error: std::error::Error,
    {
        let tls = MaybeTlsSettings::from_config(tls, true)?;
        let path = path.to_owned();
        //TODO : get grpc auth in here
        let span = crate::trace::current_span();
        let tonic = VectorServer::new(VectorSourceService::default());
        let mut warp = warp::service(warp::path("hello").map(|| "fun"));

        let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
        Server::builder()
            .add_service(tonic)
            .serve_with_shutdown(address, shutdown.map(|token| tx.send(token).await))
            .await?;
        let shutdown_token = rx.await;
        drop(shutdown_token);
        Ok(())
    }
}

fn build_event(body: Bytes) -> Option<Event> {
    let byte_size = body.len();
    match proto::EventWrapper::decode(body).map(Event::from) {
        Ok(event) => {
            emit!(VectorEventReceived { byte_size });
            Some(event)
        }
        Err(error) => {
            emit!(VectorProtoDecodeError { error });
            None
        }
    }
}
