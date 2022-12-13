use bytes::{Bytes, BytesMut};
use futures::{FutureExt, SinkExt};
use http::{Request, Uri};
use hyper::Body;
use indoc::indoc;
use serde_json::{json, Value};
use snafu::{ResultExt, Snafu};
use tokio_util::codec::Encoder as _;
use vector_config::configurable_component;

use crate::{
    codecs::{Encoder, EncodingConfig, Transformer},
    config::{AcknowledgementsConfig, DataType, GenerateConfig, Input, SinkConfig, SinkContext},
    event::Event,
    gcp::{GcpAuthConfig, GcpAuthenticator, Scope, PUBSUB_URL},
    http::HttpClient,
    sinks::{
        gcs_common::config::healthcheck_response,
        util::{
            http::{BatchedHttpSink, HttpEventEncoder, HttpSink},
            BatchConfig, BoxedRawValue, JsonArrayBuffer, SinkBatchSettings, TowerRequestConfig,
        },
        Healthcheck, UriParseSnafu, VectorSink,
    },
    tls::{TlsConfig, TlsSettings},
};

#[derive(Debug, Snafu)]
enum HealthcheckError {
    #[snafu(display("Configured topic not found"))]
    TopicNotFound,
}

// 10MB maximum message size: https://cloud.google.com/pubsub/quotas#resource_limits
const MAX_BATCH_PAYLOAD_SIZE: usize = 10_000_000;

#[derive(Clone, Copy, Debug, Default)]
pub struct PubsubDefaultBatchSettings;

impl SinkBatchSettings for PubsubDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1000);
    const MAX_BYTES: Option<usize> = Some(10_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Configuration for the `gcp_pubsub` sink.
#[configurable_component(sink("gcp_pubsub"))]
#[derive(Clone, Debug)]
pub struct PubsubConfig {
    /// The project name to which to publish events.
    pub project: String,

    /// The topic within the project to which to publish events.
    pub topic: String,

    /// The endpoint to which to publish events.
    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default, flatten)]
    pub auth: GcpAuthConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<PubsubDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    encoding: EncodingConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for PubsubConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            project = "my-project"
            topic = "my-topic"
            encoding.codec = "json"
        "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
impl SinkConfig for PubsubConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = PubsubSink::from_config(self).await?;
        let batch_settings = self
            .batch
            .validate()?
            .limit_max_bytes(MAX_BATCH_PAYLOAD_SIZE)?
            .into_batch_settings()?;
        let request_settings = self.request.unwrap_with(&Default::default());
        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls_settings, cx.proxy())?;

        let healthcheck = healthcheck(client.clone(), sink.uri("")?, sink.auth.clone()).boxed();

        let sink = BatchedHttpSink::new(
            sink,
            JsonArrayBuffer::new(batch_settings.size),
            request_settings,
            batch_settings.timeout,
            client,
        )
        .sink_map_err(|error| error!(message = "Fatal gcp_pubsub sink error.", %error));

        Ok((VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type() & DataType::Log)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

struct PubsubSink {
    auth: GcpAuthenticator,
    uri_base: String,
    transformer: Transformer,
    encoder: Encoder<()>,
}

impl PubsubSink {
    async fn from_config(config: &PubsubConfig) -> crate::Result<Self> {
        // We only need to load the credentials if we are not targeting an emulator.
        let auth = config.auth.build(Scope::PubSub).await?;

        let uri_base = match config.endpoint.as_ref() {
            Some(host) => host.to_string(),
            None => PUBSUB_URL.into(),
        };
        let uri_base = format!(
            "{}/v1/projects/{}/topics/{}",
            uri_base, config.project, config.topic,
        );

        let transformer = config.encoding.transformer();
        let serializer = config.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);

        Ok(Self {
            auth,
            uri_base,
            transformer,
            encoder,
        })
    }

    fn uri(&self, suffix: &str) -> crate::Result<Uri> {
        let uri = format!("{}{}", self.uri_base, suffix);
        let mut uri = uri.parse::<Uri>().context(UriParseSnafu)?;
        self.auth.apply_uri(&mut uri);
        Ok(uri)
    }
}

struct PubSubSinkEventEncoder {
    transformer: Transformer,
    encoder: Encoder<()>,
}

impl HttpEventEncoder<Value> for PubSubSinkEventEncoder {
    fn encode_event(&mut self, mut event: Event) -> Option<Value> {
        self.transformer.transform(&mut event);
        let mut bytes = BytesMut::new();
        // Errors are handled by `Encoder`.
        self.encoder.encode(event, &mut bytes).ok()?;
        // Each event needs to be base64 encoded, and put into a JSON object
        // as the `data` item.
        Some(json!({ "data": base64::encode(&bytes) }))
    }
}

#[async_trait::async_trait]
impl HttpSink for PubsubSink {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;
    type Encoder = PubSubSinkEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        PubSubSinkEventEncoder {
            transformer: self.transformer.clone(),
            encoder: self.encoder.clone(),
        }
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Bytes>> {
        let body = json!({ "messages": events });
        let body = crate::serde::json::to_bytes(&body).unwrap().freeze();

        let uri = self.uri(":publish").unwrap();
        let builder = Request::post(uri).header("Content-Type", "application/json");

        let mut request = builder.body(body).unwrap();
        self.auth.apply(&mut request);

        Ok(request)
    }
}

async fn healthcheck(client: HttpClient, uri: Uri, auth: GcpAuthenticator) -> crate::Result<()> {
    let mut request = Request::get(uri).body(Body::empty()).unwrap();
    auth.apply(&mut request);

    let response = client.send(request).await?;
    healthcheck_response(response, auth, HealthcheckError::TopicNotFound.into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::stream;
    use indoc::indoc;
    use prost_types::DescriptorProto;

    use super::*;

    // prost emits some generated code that includes clones on `Arc`
    // objects, which causes a clippy ding on this block. We don't
    // directly control the generated code, so allow this lint here.
    #[allow(clippy::clone_on_ref_ptr)]
    #[allow(warnings)]
    mod bq_proto {
        include!(concat!(env!("OUT_DIR"), "/includes.rs"));
        //include!(concat!(
        //    env!("OUT_DIR"),
        //    "/google.cloud.bigquery.storage.v1.rs"
        //));
    }

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<PubsubConfig>();
    }

    #[tokio::test]
    async fn fails_missing_creds() {
        let config: PubsubConfig = toml::from_str(indoc! {r#"
                project = "project"
                topic = "topic"
                encoding.codec = "json"
            "#})
        .unwrap();
        if config.build(SinkContext::new_test()).await.is_ok() {
            panic!("config.build failed to error");
        }
    }

    fn descriptor_proto_type_from_avro_type(avro_schema: &avro_rs::Schema) -> i32 {
        use prost_types::field_descriptor_proto::Type;

        let descriptor_proto_type = match avro_schema {
            avro_rs::Schema::Null => panic!("unexpected type"),
            avro_rs::Schema::Boolean => Type::Bool,
            avro_rs::Schema::Int => Type::Int32,
            avro_rs::Schema::Long => Type::Int64,
            avro_rs::Schema::Float => Type::Float,
            avro_rs::Schema::Double => Type::Double,
            avro_rs::Schema::Bytes => Type::Bytes,
            avro_rs::Schema::String => Type::String,
            _ => panic!("unexpected type"),
            // TODO  should we try parsing some of these ?
            // avro_rs::Schema::Enum { name, doc, symbols } => todo!(),
            // avro_rs::Schema::Fixed { name, size } => todo!(),
            // avro_rs::Schema::Decimal { precision, scale, inner } => todo!(),
            // avro_rs::Schema::Uuid => todo!(),
            // avro_rs::Schema::Date => todo!(),
            // avro_rs::Schema::TimeMillis => todo!(),
            // avro_rs::Schema::TimeMicros => todo!(),
            // avro_rs::Schema::TimestampMillis => todo!(),
            // avro_rs::Schema::TimestampMicros => todo!(),
            // avro_rs::Schema::Duration => todo!(),
        };

        descriptor_proto_type as i32
    }

    #[test]
    fn big_query_foo() {
        use avro_rs::*;

        let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": ["null", "string"]}
            ]
        }
    "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        println!("{:?}", schema);

        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

        let mut record = types::Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", Some("foo"));

        writer.append(record).unwrap();
    }

    #[tokio::test]
    async fn big_query() {
        use append_rows_request::*;
        use big_query_read_client::BigQueryReadClient;
        use big_query_write_client::BigQueryWriteClient;
        use bq_proto::google::cloud::bigquery::storage::v1::*;

        let auth = GcpAuthConfig {
            credentials_path: Some("/tmp/gcp_auth.json".to_owned()),
            ..Default::default()
        }
        .build(Scope::BigQuery)
        .await
        .unwrap();

        let project = std::env::var("BQ_PROJECT").unwrap();
        let dataset = std::env::var("BQ_DATASET").unwrap();
        let table = std::env::var("BQ_TABLE").unwrap();

        let table_path =
            format!("projects/{}/datasets/{}/tables/{}", project, dataset, table).to_owned();

        // use the Storage Read API to obtain the schema

        let mut field_descriptors = vec![];

        let mut schema = None;

        // read to get schema
        {
            // create_client {
            let channel =
                tonic::transport::Endpoint::from_static("https://bigquerystorage.googleapis.com")
                    .connect()
                    .await
                    .unwrap();

            let mut client =
                BigQueryReadClient::with_interceptor(channel, |mut req: tonic::Request<()>| {
                    if let Some(token) = auth.make_token() {
                        let authorization = tonic::metadata::MetadataValue::try_from(&token)
                            .map_err(|_| {
                                tonic::Status::new(
                                    tonic::Code::FailedPrecondition,
                                    "Invalid token text returned by GCP",
                                )
                            })
                            .unwrap();
                        req.metadata_mut().insert("authorization", authorization);
                    }
                    Ok(req)
                });
            // } create_client

            let read_request = CreateReadSessionRequest {
                parent: format!("projects/{}", project).to_owned(),
                read_session: Some(ReadSession {
                    name: "".to_owned(),
                    expire_time: None,
                    data_format: DataFormat::Avro as i32,
                    table: table_path.clone(),
                    table_modifiers: None,
                    read_options: None,
                    streams: vec![],
                    estimated_total_bytes_scanned: 0,
                    trace_id: "".to_owned(),
                    schema: None,
                }),
                max_stream_count: 1,
                preferred_min_stream_count: 1,
            };

            match client.create_read_session(read_request).await {
                Ok(mut tonic_response) => {
                    // get the schema from the response
                    let read_session = tonic_response.get_mut();
                    let schema_ = read_session.schema.as_ref().unwrap();

                    // should be avro format since we specified that
                    let schema_ = match schema_ {
                        read_session::Schema::AvroSchema(s) => &s.schema,
                        read_session::Schema::ArrowSchema(_) => panic!("asked for avro schema"),
                    };

                    schema = Some(avro_rs::Schema::parse_str(schema_.as_str()).unwrap());

                    // create the FieldDescriptorProto from the Avro schema fields.
                    //
                    let (_name, _doc, fields, _lookup) = match schema.as_ref().unwrap() {
                        avro_rs::Schema::Record {
                            name,
                            doc,
                            fields,
                            lookup,
                        } => (name, doc, fields, lookup),
                        _ => panic!("unexpected Avro schema format"),
                    };

                    for field in fields {
                        println!("");
                        println!("field: {:?}", field);
                        println!("");
                        let union_schema = match &field.schema {
                            avro_rs::Schema::Union(union_schema) => union_schema,
                            _ => panic!("unexpected Avro schema format"),
                        };

                        let t = if union_schema.is_nullable() {
                            &union_schema.variants()[1]
                        } else {
                            &union_schema.variants()[0]
                        };

                        let type_ = descriptor_proto_type_from_avro_type(t);

                        let descriptor = prost_types::FieldDescriptorProto {
                            name: Some(field.name.to_owned()),
                            number: Some(field.position as i32 + 1),
                            label: None,
                            r#type: Some(type_),
                            type_name: None,
                            extendee: None,

                            // TODO parse the JSON value type to the string version of that type as
                            // defined in https://docs.rs/prost-types/latest/prost_types/struct.FieldDescriptorProto.html#structfield.default_value
                            //default_value: field.default,
                            default_value: None,
                            oneof_index: None,
                            json_name: None,
                            options: None,
                            proto3_optional: None,
                        };

                        field_descriptors.push(descriptor);
                    }
                }
                Err(e) => {
                    println!("error creating read session: {}", e);
                }
            }
        } // read to get schema

        let encoded_row = if let Some(schema) = schema {
            println!("schema_is: {:?}", schema);

            let mut writer = avro_rs::Writer::new(&schema, Vec::new());
            let mut record = avro_rs::types::Record::new(writer.schema()).unwrap();

            // BIG NOTE !!
            //
            // The avro_rs crate does not document this AT ALL but, if the mode is nullable , aka
            // the type is a list and the first entry in the list is Null , then you have to make
            // the values in the put call as Option types.

            record.put("weight_kg", Some(avro_rs::types::Value::Long(0)));
            record.put(
                "driver",
                Some(avro_rs::types::Value::String("lando".to_owned())),
            );
            record.put("points", Some(avro_rs::types::Value::Long(1)));
            record.put("year", Some(avro_rs::types::Value::Long(2022)));
            record.put(
                "team",
                Some(avro_rs::types::Value::String("haas".to_owned())),
            );
            record.put("height_meters", Some(avro_rs::types::Value::Double(3.2)));
            record.put("rank", Some(avro_rs::types::Value::Long(2)));

            for f in &record.fields {
                println!("field k: {:?}, v: {:?}", f.0, f.1);
            }

            // validation against schema occurs here
            match writer.append(record) {
                Ok(_) => {}
                Err(e) => panic!("error writing record: {}", e),
            }

            let encoded = writer.into_inner().unwrap();

            encoded
        } else {
            vec![]
        };

        // AppendRows RPC (loop)
        {
            // create_client {
            let channel =
                tonic::transport::Endpoint::from_static("https://bigquerystorage.googleapis.com")
                    .connect()
                    .await
                    .unwrap();

            let mut client =
                BigQueryWriteClient::with_interceptor(channel, |mut req: tonic::Request<()>| {
                    if let Some(token) = auth.make_token() {
                        let authorization = tonic::metadata::MetadataValue::try_from(&token)
                            .map_err(|_| {
                                tonic::Status::new(
                                    tonic::Code::FailedPrecondition,
                                    "Invalid token text returned by GCP",
                                )
                            })
                            .unwrap();
                        req.metadata_mut().insert("authorization", authorization);
                    }
                    Ok(req)
                });
            // } create_client

            // CreateWriteStream doesn't need to be called for the _default stream

            let write_path = format!(
                "projects/{}/datasets/{}/tables/{}/streams/_default",
                project, dataset, table
            )
            .to_owned();

            let request = stream::iter(vec![AppendRowsRequest {
                write_stream: write_path,
                offset: None,
                trace_id: "".to_owned(),
                missing_value_interpretations: BTreeMap::new(),
                rows: Some(Rows::ProtoRows(ProtoData {
                    writer_schema: Some(ProtoSchema {
                        // TODO, the below needs to be filled out (only on the first request) for writes to succeed.
                        // wondering if can get from Read request.
                        proto_descriptor: Some(DescriptorProto {
                            name: Some("ProtoData".to_owned()),
                            field: field_descriptors, // Vec<FieldDescriptorProto>
                            extension: vec![],        // Vec<FieldDescriptorProto>
                            nested_type: vec![],      // Vec<DescriptorProto>
                            enum_type: vec![],        // Vec<EnumDescriptorProto>
                            extension_range: vec![],  // Vec<ExtensionRange>
                            oneof_decl: vec![],       // Vec<OneOfDescriptorProto>
                            options: None,
                            //options: Some(MessageOptions {
                            //    message_set_wire_format: Some(true),
                            //    deprecated: Some(false),
                            //    map_entry: Some(true),
                            //    uninterpreted_option: vec![], // Vec<UninterpretedOption>
                            //    no_standard_descriptor_accessor: Some(true),
                            //}), // MessageOptions
                            reserved_range: vec![], // Vec<ReservedRange>
                            reserved_name: vec![],  // Vec<String>
                        }),
                    }),
                    rows: Some(ProtoRows {
                        serialized_rows: vec![encoded_row],
                    }),
                })),
            }]);

            // below will fail until schema is correct

            match client.append_rows(request).await {
                Ok(mut tonic_response) => {
                    let streaming = tonic_response.get_mut();
                    loop {
                        match streaming.message().await {
                            Ok(Some(append_rows_response)) => {
                                println!("append_rows_response: {:?}", append_rows_response);
                            }
                            Ok(None) => {
                                println!("stream closed naturally");
                                break;
                            }
                            Err(status) => {
                                println!("gRPC error: {:?}", status);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("error appending rows: {}", e);
                }
            };
        } // AppendRows

        // FinalizeWriteStream is not supported on the _default stream
    }
}

#[cfg(all(test, feature = "gcp-integration-tests"))]
mod integration_tests {
    use codecs::JsonSerializerConfig;
    use reqwest::{Client, Method, Response};
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};
    use vector_core::event::{BatchNotifier, BatchStatus};

    use super::*;
    use crate::gcp;
    use crate::test_util::components::{run_and_assert_sink_error, COMPONENT_ERROR_TAGS};
    use crate::test_util::{
        components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
        random_events_with_stream, random_string, trace_init,
    };

    const PROJECT: &str = "testproject";

    fn config(topic: &str) -> PubsubConfig {
        PubsubConfig {
            project: PROJECT.into(),
            topic: topic.into(),
            endpoint: Some(gcp::PUBSUB_ADDRESS.clone()),
            auth: GcpAuthConfig {
                skip_authentication: true,
                ..Default::default()
            },
            batch: Default::default(),
            request: Default::default(),
            encoding: JsonSerializerConfig::new().into(),
            tls: Default::default(),
            acknowledgements: Default::default(),
        }
    }

    async fn config_build(topic: &str) -> (VectorSink, crate::sinks::Healthcheck) {
        let cx = SinkContext::new_test();
        config(topic).build(cx).await.expect("Building sink failed")
    }

    #[tokio::test]
    async fn publish_events() {
        trace_init();

        let (topic, subscription) = create_topic_subscription().await;
        let (sink, healthcheck) = config_build(&topic).await;

        healthcheck.await.expect("Health check failed");

        let (batch, mut receiver) = BatchNotifier::new_with_receiver();
        let (input, events) = random_events_with_stream(100, 100, Some(batch));
        run_and_assert_sink_compliance(sink, events, &HTTP_SINK_TAGS).await;
        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));

        let response = pull_messages(&subscription, 1000).await;
        let messages = response
            .receivedMessages
            .as_ref()
            .expect("Response is missing messages");
        assert_eq!(input.len(), messages.len());
        for i in 0..input.len() {
            let data = messages[i].message.decode_data();
            let data = serde_json::to_value(data).unwrap();
            let expected = serde_json::to_value(input[i].as_log().all_fields().unwrap()).unwrap();
            assert_eq!(data, expected);
        }
    }

    #[tokio::test]
    async fn publish_events_broken_topic() {
        trace_init();

        let (topic, _subscription) = create_topic_subscription().await;
        let (sink, _healthcheck) = config_build(&format!("BREAK{}BREAK", topic)).await;
        // Explicitly skip healthcheck

        let (batch, mut receiver) = BatchNotifier::new_with_receiver();
        let (_input, events) = random_events_with_stream(100, 100, Some(batch));
        run_and_assert_sink_error(sink, events, &COMPONENT_ERROR_TAGS).await;
        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Rejected));
    }

    #[tokio::test]
    async fn checks_for_valid_topic() {
        trace_init();

        let (topic, _subscription) = create_topic_subscription().await;
        let topic = format!("BAD{}", topic);
        let (_sink, healthcheck) = config_build(&topic).await;
        healthcheck.await.expect_err("Health check did not fail");
    }

    async fn create_topic_subscription() -> (String, String) {
        let topic = format!("topic-{}", random_string(10));
        let subscription = format!("subscription-{}", random_string(10));
        request(Method::PUT, &format!("topics/{}", topic), json!({}))
            .await
            .json::<Value>()
            .await
            .expect("Creating new topic failed");
        request(
            Method::PUT,
            &format!("subscriptions/{}", subscription),
            json!({ "topic": format!("projects/{}/topics/{}", PROJECT, topic) }),
        )
        .await
        .json::<Value>()
        .await
        .expect("Creating new subscription failed");
        (topic, subscription)
    }

    async fn request(method: Method, path: &str, json: Value) -> Response {
        let url = format!("{}/v1/projects/{}/{}", *gcp::PUBSUB_ADDRESS, PROJECT, path);
        Client::new()
            .request(method.clone(), &url)
            .json(&json)
            .send()
            .await
            .unwrap_or_else(|_| panic!("Sending {} request to {} failed", method, url))
    }

    async fn pull_messages(subscription: &str, count: usize) -> PullResponse {
        request(
            Method::POST,
            &format!("subscriptions/{}:pull", subscription),
            json!({
                "returnImmediately": true,
                "maxMessages": count
            }),
        )
        .await
        .json::<PullResponse>()
        .await
        .expect("Extracting pull data failed")
    }

    #[derive(Debug, Deserialize)]
    #[allow(non_snake_case)]
    struct PullResponse {
        receivedMessages: Option<Vec<PullMessageOuter>>,
    }

    #[derive(Debug, Deserialize)]
    #[allow(non_snake_case)]
    #[allow(dead_code)] // deserialize all fields
    struct PullMessageOuter {
        ackId: String,
        message: PullMessage,
    }

    #[derive(Debug, Deserialize)]
    #[allow(non_snake_case)]
    #[allow(dead_code)] // deserialize all fields
    struct PullMessage {
        data: String,
        messageId: String,
        publishTime: String,
    }

    impl PullMessage {
        fn decode_data(&self) -> TestMessage {
            let data = base64::decode(&self.data).expect("Invalid base64 data");
            let data = String::from_utf8_lossy(&data);
            serde_json::from_str(&data).expect("Invalid message structure")
        }
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct TestMessage {
        timestamp: String,
        message: String,
    }
}
