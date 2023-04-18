
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

// /// The port for the http server to receive data from the agent
// fn server_port_for_agent() -> u16 {
//     std::env::var("AGENT_PORT")
//         .unwrap_or_else(|_| "8082".to_string())
//         .parse::<u16>()
//         .unwrap()
// }

/// The port for the http server to receive data from vector
//const fn server_port_for_vector() -> u16 {
//    1234
//}

// /// The agent url to post metrics to [Agent only]
// fn agent_only_url() -> String {
//     std::env::var("AGENT_URL").unwrap_or_else(|_| "http://127.0.0.1:8126/api/v2/series".to_owned())
// }
//
// /// The agent url to post metrics to [Agent -> Vector].
// fn agent_to_vector_url() -> String {
//     std::env::var("AGENT_TO_VECTOR_URL")
//         .unwrap_or_else(|_| "http://127.0.0.1:8126/api/v2/series".to_owned())
// }

//fn dogstastd_socket_agent_only() -> String {
//    std::env::var("AGENT_ONLY_DOGSTASTD_SOCKET")
//        .unwrap_or_else(|_| "tests/data/datadog/dd_dogstatsd_agent_only.socket".to_owned())
//}
//

//fn dogstastd_socket_agent_vector() -> String {
//    std::env::var("AGENT_VECTOR_DOGSTASTD_SOCKET")
//        .unwrap_or_else(|_| "tests/data/datadogdd_dogstatsd_agent_vector.socket".to_owned())
//}

// /// Shared state for the HTTP server
// struct AppState {
//     tx: Sender<()>,
// }
//
// /// Runs an HTTP server on the specified port.
// async fn run_server(name: String, port: u16, tx: Sender<()>) {
//     let state = Arc::new(AppState { tx });
//     let app = Router::new()
//         .route("/api/v1/validate", get(validate))
//         .route("/api/v2/series", post(process_metrics))
//         .layer(Extension(state));
//
//     let addr = SocketAddr::from(([0, 0, 0, 0], port));
//
//     println!("HTTP server for `{}` listening on {}", name, addr);
//
//     axum::Server::bind(&addr)
//         .serve(app.into_make_service())
//         .await
//         .unwrap();
// }
//
// // Needed for the sink healthcheck
// async fn validate() -> &'static str {
//     ""
// }
//
// async fn process_metrics(Extension(state): Extension<Arc<AppState>>, request: Request<Body>) {
//     println!("got metrics: {:?}", request);
//     state.tx.send(()).await.unwrap();
// }

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

// #[derive(Serialize)]
// struct ApiMetric {
//     series: Vec<Series>,
// }
//
// #[derive(Serialize)]
// struct Series {
//     interval: Option<i64>,
//     metadata: Option<Metadata>,
//     metric: String,
//     points: Vec<Point>,
//     resources: Option<Vec<Resources>>,
//     source_type_name: Option<String>,
//     tags: Option<Vec<String>>,
//     r#type: MetricType,
//     unit: Option<String>,
// }
//
// #[derive(Serialize)]
// struct Metadata {
//     origin: Origin,
// }
//
// #[derive(Serialize)]
// struct Origin {
//     metric_type: i32,
//     product: i32,
//     service: i32,
// }
//
// #[derive(Serialize)]
// struct Point {
//     timestamp: i64,
//     value: f64,
// }
//
// #[derive(Serialize)]
// struct Resources {
//     name: String,
//     r#type: String,
// }
//
// #[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
// #[repr(u8)]
// enum MetricType {
//     Unspecified = 0,
//     Count = 1,
//     Rate = 2,
//     Gauge = 3,
// }

// sends a metric to each of the urls
//async fn send_metric(urls: &Vec<String>, payload: ApiMetric) {
//    let client = reqwest::Client::new();
//
//    let api_key = std::env::var("TEST_DATADOG_API_KEY")
//        .expect("couldn't find the Datadog api key in environment variables");
//
//    for url in urls {
//        let res = client
//            .post(url)
//            .header(ACCEPT, "application/json")
//            .header(CONTENT_TYPE, "application/json")
//            .header("DD-API-KEY", api_key.clone())
//            .json(&payload)
//            .send()
//            .await;
//
//        match res {
//            Ok(response) => {
//                if response.status() != hyper::StatusCode::OK {
//                    panic!(
//                        "Error submitting metrics to the Agent at {} : {:?}",
//                        url, response
//                    );
//                }
//                info!("Sent a metric to the Agent at {}.", url);
//            }
//            Err(e) => {
//                panic!("Error submitting metrics to the Agent at {} : {:?}", url, e);
//            }
//        }
//    }
//}
//
//async fn send_count_to_agents(urls: &Vec<String>) {
//    let count = ApiMetric {
//        series: vec![Series {
//            interval: None,
//            metadata: None,
//            metric: "foo_count".to_owned(),
//            points: vec![],
//            resources: None,
//            source_type_name: None,
//            tags: None,
//            r#type: MetricType::Count,
//            unit: None,
//        }],
//    };
//
//    send_metric(urls, count).await
//}

//async fn send_count_to_agents(paths: &Vec<String>) {
//    paths
//        .iter()
//        .map(|path| {
//            std::process::Command::new("sh")
//                .arg("-C")
//                .arg("tests/data/datadog/send_count_metrics.sh")
//                .arg(&path)
//                .spawn()
//                .expect("sh command failed to start")
//        })
//        .next();
//}

//async fn receive_the_stats(
//    rx_agent_only: &mut Receiver<()>,
//    rx_agent_vector: &mut Receiver<()>,
//) -> ((), ()) {
//    let timeout = sleep(Duration::from_secs(60));
//    tokio::pin!(timeout);
//
//    let mut stats_agent_vector = None;
//    let mut stats_agent_only = None;
//
//    // wait on the receive of stats payloads. expect one from agent, two from vector.
//    // The second payload from vector should be the aggregate.
//    loop {
//        tokio::select! {
//            d1 = rx_agent_vector.recv() => {
//                stats_agent_vector = d1;
//                if stats_agent_only.is_some() && stats_agent_vector.is_some() {
//                    break;
//                }
//            },
//            d2 = rx_agent_only.recv() => {
//                stats_agent_only = d2;
//                if stats_agent_vector.is_some() && stats_agent_only.is_some() {
//                    break;
//                }
//            },
//            _ = &mut timeout => break,
//        }
//    }
//
//    assert!(
//        stats_agent_vector.is_some(),
//        "received no payloads from vector"
//    );
//    assert!(
//        stats_agent_only.is_some(),
//        "received no payloads from agent"
//    );
//
//    (stats_agent_only.unwrap(), stats_agent_vector.unwrap())
//}

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

async fn get_fakeintake_payloads(base: &str) -> Payloads {

    let url = format!("{base}/fakeintake/payloads?endpoint=/api/v2/series");

    Client::new()
        .request(Method::GET, &url)
        .send()
        .await
        .unwrap_or_else(|_| panic!("Sending GET request to {} failed", url))
        .json::<Payloads>()
        .await
        .expect("Parsing fakeintake payloads failed")
}

async fn get_payloads_agent() -> Payloads {
    get_fakeintake_payloads(&fake_intake_agent_endpoint()).await
}

async fn _get_payloads_vector() -> Payloads {
    get_fakeintake_payloads(&fake_intake_vector_endpoint()).await
}

#[tokio::test]
async fn foo() {
    trace_init();

    // channels for the servers to send us back data on
    //let (tx_agent_vector, mut rx_agent_vector) = mpsc::channel(32);
    //let (tx_agent_only, mut rx_agent_only) = mpsc::channel(32);

    // spawn the servers
    //{
    //    // [vector -> the server]
    //    tokio::spawn(async move {
    //        run_server(
    //            "vector".to_string(),
    //            server_port_for_vector(),
    //            tx_agent_vector,
    //        )
    //        .await;
    //    });

    //    // [agent -> the server]
    //    tokio::spawn(async move {
    //        run_server("agent".to_string(), server_port_for_agent(), tx_agent_only).await;
    //    });
    //}

    // allow the Agent containers to start up
    //sleep(Duration::from_secs(15)).await;

    // starts the vector source and sink
    // panics if vector errors during startup
    let (_topology, _shutdown) = start_vector().await;

    // the URLs of the Agent metrics endpoints that metrics will be sent to
    //let sockets = vec![
    //    dogstastd_socket_agent_only(),
    //    dogstastd_socket_agent_vector(),
    //];

    // send the metrics through the agent containers and panic if any of the HTTP posts fail.
    //send_count_to_agents(&sockets).await;

    // receive the stats on the channel receivers from the servers
    //let (_stats_agent, _stats_vector) =
    //    receive_the_stats(&mut rx_agent_only, &mut rx_agent_vector).await;

    sleep(Duration::from_secs(10)).await;

    let agent_payloads = get_payloads_agent().await;

    //let vector_payloads = get_payloads_vector().await;

    let payloads = &agent_payloads.payloads;

    for payload in payloads {

        println!("{{");
        println!("    {:?}", &payload.timestamp);
        println!();

        //println!("{:?}", &payloads.encoding);
        //println!();

        //println!("raw payload: {:?}", &payloads[0].data);
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
