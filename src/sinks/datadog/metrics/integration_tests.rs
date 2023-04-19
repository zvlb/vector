
//use axum::{
//    body::Body,
//    extract::Extension,
//    http::Request,
//    routing::{get, post},
//    Router,
//};
use bytes::Bytes;
use flate2::read::ZlibDecoder;
use futures::{channel::mpsc::Receiver as FReceiver, stream, StreamExt};
use hyper::StatusCode;
use indoc::indoc;
use prost::Message;
use rand::{thread_rng, Rng};
//use serde::Serialize;
//use serde_repr::{Deserialize_repr, Serialize_repr};
//use std::{net::SocketAddr, sync::Arc};
//use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use super::DatadogMetricsConfig;
use vector_core::event::{BatchNotifier, BatchStatus, Event, Metric, MetricKind, MetricValue};
use reqwest::{Client, Method};
use serde::Deserialize;
use base64::prelude::{Engine as _, BASE64_STANDARD};


use crate::{
    config::{ConfigBuilder, SinkConfig},
    sinks::util::test::{build_test_server_status, load_sink},
    sources::datadog_agent::DatadogAgentConfig,
    test_util::{
        components::{assert_sink_compliance, SINK_TAGS},
        map_event_batch_stream, next_addr, start_topology, trace_init,
    },
    topology::RunningTopology,
};

use crate::sources::datadog_agent::ddmetric_proto::MetricPayload;
//use crate::common::datadog::DatadogSeriesMetric;

enum ApiStatus {
    OK,
    // Forbidden,
}

fn test_server(
    addr: std::net::SocketAddr,
    api_status: ApiStatus,
) -> (
    FReceiver<(http::request::Parts, Bytes)>,
    stream_cancel::Trigger,
    impl std::future::Future<Output = Result<(), ()>>,
) {
    let status = match api_status {
        ApiStatus::OK => StatusCode::OK,
        // ApiStatus::Forbidden => StatusCode::FORBIDDEN,
    };

    // NOTE: we pass `Trigger` out to the caller even though this suite never
    // uses it as it's being dropped cancels the stream machinery here,
    // indicating failures that might not be valid.
    build_test_server_status(addr, status)
}

/// Starts a test sink with random metrics running into it
///
/// This function starts a Datadog Metrics sink with a simplistic configuration and
/// runs random lines through it, returning a vector of the random lines and a
/// Receiver populated with the result of the sink's operation.
///
/// Testers may set `http_status` and `batch_status`. The first controls what
/// status code faked HTTP responses will have, the second acts as a check on
/// the `Receiver`'s status before being returned to the caller.
async fn start_test(
    api_status: ApiStatus,
    batch_status: BatchStatus,
) -> (Vec<Event>, FReceiver<(http::request::Parts, Bytes)>) {
    let config = indoc! {r#"
        default_api_key = "atoken"
        default_namespace = "foo"
    "#};
    let (mut config, cx) = load_sink::<DatadogMetricsConfig>(config).unwrap();

    let addr = next_addr();
    // Swap out the endpoint so we can force send it
    // to our local server
    let endpoint = format!("http://{}", addr);
    config.dd_common.endpoint = Some(endpoint.clone());

    let (sink, _) = config.build(cx).await.unwrap();

    let (rx, _trigger, server) = test_server(addr, api_status);
    tokio::spawn(server);

    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<_> = (0..10)
        .map(|index| {
            Event::Metric(Metric::new(
                format!("counter_{}", thread_rng().gen::<u32>()),
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: index as f64,
                },
            ))
        })
        .collect();
    let stream = map_event_batch_stream(stream::iter(events.clone()), Some(batch));

    sink.run(stream).await.unwrap();
    assert_eq!(receiver.await, batch_status);

    (events, rx)
}

fn decompress_payload(payload: Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut decompressor = ZlibDecoder::new(&payload[..]);
    let mut decompressed = Vec::new();
    let result = std::io::copy(&mut decompressor, &mut decompressed);
    result.map(|_| decompressed)
}

#[tokio::test]
/// Assert the basic functionality of the sink in good conditions
///
/// This test rigs the sink to return OK to responses, checks that all batches
/// were delivered and then asserts that every message is able to be
/// deserialized.
async fn smoke() {
    let (expected, rx) = start_test(ApiStatus::OK, BatchStatus::Delivered).await;

    let output = rx.take(expected.len()).collect::<Vec<_>>().await;

    for val in output.iter() {
        assert_eq!(
            val.0.headers.get("Content-Type").unwrap(),
            "application/json"
        );
        assert_eq!(val.0.headers.get("DD-API-KEY").unwrap(), "atoken");
        assert!(val.0.headers.contains_key("DD-Agent-Payload"));

        let compressed_payload = val.1.to_vec();
        let payload = decompress_payload(compressed_payload).unwrap();
        let payload = std::str::from_utf8(&payload).unwrap();
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();

        let series = payload
            .as_object()
            .unwrap()
            .get("series")
            .unwrap()
            .as_array()
            .unwrap();
        assert!(!series.is_empty());

        // check metrics are sorted by name, which helps HTTP compression
        let metric_names: Vec<String> = series
            .iter()
            .map(|value| {
                value
                    .as_object()
                    .unwrap()
                    .get("metric")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let mut sorted_names = metric_names.clone();
        sorted_names.sort();
        assert_eq!(metric_names, sorted_names);

        let entry = series.first().unwrap().as_object().unwrap();
        assert_eq!(
            entry.get("metric").unwrap().as_str().unwrap(),
            "foo.counter"
        );
        assert_eq!(entry.get("type").unwrap().as_str().unwrap(), "count");
        let points = entry
            .get("points")
            .unwrap()
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points.get(1).unwrap().as_f64().unwrap(), 1.0);
    }
}

#[tokio::test]
async fn real_endpoint() {
    assert_sink_compliance(&SINK_TAGS, async {
        let config = indoc! {r#"
        default_api_key = "${TEST_DATADOG_API_KEY}"
        default_namespace = "fake.test.integration"
    "#};
        let api_key = std::env::var("TEST_DATADOG_API_KEY").unwrap();
        assert!(!api_key.is_empty(), "$TEST_DATADOG_API_KEY required");
        let config = config.replace("${TEST_DATADOG_API_KEY}", &api_key);
        let (config, cx) = load_sink::<DatadogMetricsConfig>(config.as_str()).unwrap();

        let (sink, _) = config.build(cx).await.unwrap();
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let events: Vec<_> = (0..10)
            .map(|index| {
                Event::Metric(Metric::new(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter {
                        value: index as f64,
                    },
                ))
            })
            .collect();
        let stream = map_event_batch_stream(stream::iter(events.clone()), Some(batch));

        sink.run(stream).await.unwrap();
        assert_eq!(receiver.await, BatchStatus::Delivered);
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn vector_receive_port() -> u16 {
    std::env::var("VECTOR_RECEIVE_PORT")
        .unwrap_or_else(|_| "8081".to_string())
        .parse::<u16>()
        .unwrap()
}

fn fake_intake_vector_endpoint() -> String {
    std::env::var("FAKE_INTAKE_VECTOR_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8082".to_string())
}

fn fake_intake_agent_endpoint() -> String {
    std::env::var("FAKE_INTAKE_AGENT_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8083".to_string())
}

async fn start_vector() -> (
    RunningTopology,
    (mpsc::UnboundedSender<()>, mpsc::UnboundedReceiver<()>),
) {
    let dd_agent_address = format!("0.0.0.0:{}", vector_receive_port());

    let source_config = toml::from_str::<DatadogAgentConfig>(&format!(
        indoc! { r#"
            address = "{}"
            multiple_outputs = true
        "#},
        dd_agent_address,
    ))
    .unwrap();

    let mut builder = ConfigBuilder::default();
    builder.add_source("in", source_config);

    let dd_metrics_endpoint =  fake_intake_vector_endpoint();
    let cfg = format!(
        indoc! { r#"
            default_api_key = "atoken"
            endpoint = "{}"
        "#},
        dd_metrics_endpoint
    );

    // TODO I don't think we actually need this API key to be set since we're not exporting to DD.
    let api_key = std::env::var("TEST_DATADOG_API_KEY")
        .expect("couldn't find the Datadog api key in environment variables");
    assert!(!api_key.is_empty(), "TEST_DATADOG_API_KEY required");
    let cfg = cfg.replace("atoken", &api_key);

    let sink_config = toml::from_str::<DatadogMetricsConfig>(&cfg).unwrap();

    builder.add_sink("out", &["in.metrics"], sink_config);

    let config = builder.build().expect("building config should not fail");

    let (topology, shutdown) = start_topology(config, false).await;
    println!("Started vector.");

    (topology, shutdown)
}

#[derive(Deserialize, Debug)]
struct Payloads {
    payloads: Vec<Payload>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Payload {
    data: String,
    encoding: String,
    timestamp: String,
}

async fn get_fakeintake_payloads(url: &str) -> Payloads {

    Client::new()
        .request(Method::GET, url)
        .send()
        .await
        .unwrap_or_else(|_| panic!("Sending GET request to {} failed", url))
        .json::<Payloads>()
        .await
        .expect("Parsing fakeintake payloads failed")
}

async fn get_payloads_agent() -> Payloads {
    let url = format!("{}/fakeintake/payloads?endpoint=/api/v2/series", fake_intake_agent_endpoint());
    get_fakeintake_payloads(&url).await
}

async fn get_payloads_vector() -> Payloads {
    let url = format!("{}/fakeintake/payloads?endpoint=/api/v1/series", fake_intake_vector_endpoint());
    get_fakeintake_payloads(&url).await
}

fn print_payloads_agent(payloads: &Vec<Payload>) {
    println!("received {} payloads", payloads.len());

    for payload in payloads {

        println!("{{");
        //println!("    {:?}", &payload.timestamp);
        //println!();

        //println!("{:?}", &payload.encoding);
        //println!();

        //println!("raw payload: {:?}", &payload.data);
        //println!();

        // decode base64
        let payload = BASE64_STANDARD
            .decode(&payload.data)
            .expect("Invalid base64 data");

        // decompress
        let bytes = Bytes::from(decompress_payload(payload).unwrap());

        let payload = MetricPayload::decode(bytes).unwrap();

        for series in payload.series {
            if series.metric.contains("foo_metric") {
                println!("    {:?}", series);
            }
        }

        println!("}}");
    }
}

fn print_payloads_vector(payloads: &Vec<Payload>) {
    println!("received {} payloads", payloads.len());

    for payload in payloads {

        println!("{{");
        //println!("    {:?}", &payload.timestamp);
        //println!();

        //println!("{:?}", &payload.encoding);
        //println!();

        //println!("raw payload: {:?}", &payload.data);
        //println!();

        // decode base64
        let payload = BASE64_STANDARD
            .decode(&payload.data)
            .expect("Invalid base64 data");

        let payload = decompress_payload(payload).unwrap();
        let payload = std::str::from_utf8(&payload).unwrap();
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();

        //println!("decoded, decompressed payload: {:?}", &payload);
        //println!();

        let series = payload
            .as_object()
            .unwrap()
            .get("series")
            .unwrap()
            .as_array()
            .unwrap();

        for serie in series {
            let metric = serie.get("metric").unwrap().as_str().unwrap();
            if metric == "foo_metric" {
                println!("    {:?}", serie);
            }
        }

        println!("}}");
    }
}

#[tokio::test]
async fn foo() {
    trace_init();

    // starts the vector source and sink
    // panics if vector errors during startup
    let (_topology, _shutdown) = start_vector().await;

    sleep(Duration::from_secs(25)).await;

    let agent_payloads = get_payloads_agent().await;
    println!("AGENT PAYLOADS");
    println!();
    print_payloads_agent(&agent_payloads.payloads);

    let vector_payloads = get_payloads_vector().await;
    println!("VECTOR PAYLOADS");
    println!();
    print_payloads_vector(&vector_payloads.payloads);
}
