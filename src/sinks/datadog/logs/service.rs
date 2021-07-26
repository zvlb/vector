use crate::sinks::datadog::logs::config::Encoding;
use crate::sinks::datadog::ApiKey;
use crate::sinks::util::buffer::GZIP_FAST;
use crate::sinks::util::encoding::EncodingConfigWithDefault;
use crate::sinks::util::encoding::EncodingConfiguration;
use crate::sinks::util::http::HttpSink;
use crate::sinks::util::Compression;
use crate::sinks::util::EncodedLength;
use crate::sinks::util::{EncodedEvent, PartitionInnerBuffer};
use crate::{config::log_schema, internal_events::DatadogLogEventProcessed};
use flate2::write::GzEncoder;
use http::Request;
use http::Uri;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;
use vector_core::config::LogSchema;
use vector_core::event::{Event, Value};

#[derive(Debug, Default)]
pub(crate) struct ServiceBuilder {
    uri: Option<Uri>,
    default_api_key: Option<ApiKey>,
    compression: Compression,
    encoding: Option<EncodingConfigWithDefault<Encoding>>,
    log_schema_message_key: Option<&'static str>,
    log_schema_timestamp_key: Option<&'static str>,
    log_schema_host_key: Option<&'static str>,
}

impl ServiceBuilder {
    pub(crate) fn uri(mut self, uri: Uri) -> Self {
        self.uri = Some(uri);
        self
    }

    pub(crate) fn default_api_key(mut self, api_key: ApiKey) -> Self {
        self.default_api_key = Some(api_key);
        self
    }

    pub(crate) fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub(crate) fn encoding(mut self, encoding: EncodingConfigWithDefault<Encoding>) -> Self {
        self.encoding = Some(encoding);
        self
    }

    pub(crate) fn log_schema(mut self, log_schema: &'static LogSchema) -> Self {
        self.log_schema_host_key = Some(log_schema.host_key());
        self.log_schema_message_key = Some(log_schema.message_key());
        self.log_schema_timestamp_key = Some(log_schema.timestamp_key());
        self
    }

    pub(crate) fn build(self) -> Service {
        Service {
            uri: self.uri.expect("must set URI"),
            default_api_key: self
                .default_api_key
                .expect("must set a default Datadog API key"),
            compression: self.compression,
            encoding: self.encoding.expect("must set an encoding"),
            log_schema_host_key: self
                .log_schema_host_key
                .unwrap_or_else(|| log_schema().host_key()),
            log_schema_message_key: self
                .log_schema_message_key
                .unwrap_or_else(|| log_schema().message_key()),
            log_schema_timestamp_key: self
                .log_schema_timestamp_key
                .unwrap_or_else(|| log_schema().timestamp_key()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Service {
    uri: Uri,
    default_api_key: ApiKey,
    compression: Compression,
    encoding: EncodingConfigWithDefault<Encoding>,
    log_schema_message_key: &'static str,
    log_schema_timestamp_key: &'static str,
    log_schema_host_key: &'static str,
}

impl Service {
    pub(crate) fn builder() -> ServiceBuilder {
        ServiceBuilder::default()
    }
}

/// https://docs.datadoghq.com/api/latest/logs/#send-logs
#[derive(Serialize, Clone)]
pub(crate) struct LogMsg {
    ddsource: Value,
    ddtags: Value,
    hostname: Value,
    message: Value,
    service: Value,

    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

pub(crate) enum ConversionError {
    MissingField(&'static str),
}

impl TryFrom<BTreeMap<String, Value>> for LogMsg {
    type Error = ConversionError;

    fn try_from(mut fields: BTreeMap<String, Value>) -> Result<Self, Self::Error> {
        let ddsource = fields
            .remove("ddsource")
            .ok_or_else(|| ConversionError::MissingField("ddsource"))?
            .into();
        let ddtags = fields
            .remove("ddtags")
            .ok_or_else(|| ConversionError::MissingField("ddtags"))?;
        let hostname = fields
            .remove("hostname")
            .ok_or_else(|| ConversionError::MissingField("hostname"))?;
        let message = fields
            .remove("message")
            .ok_or_else(|| ConversionError::MissingField("message"))?;
        let service = fields
            .remove("service")
            .ok_or_else(|| ConversionError::MissingField("service"))?;

        Ok(LogMsg {
            ddsource,
            ddtags,
            hostname,
            message,
            service,
            extra: fields,
        })
    }
}

const fn kv_len(key_len: usize, val_len: usize) -> usize {
    let quotes = 4;
    let spaces = 2;
    let colon = 1;
    let comma = 1;

    key_len + val_len + quotes + spaces + colon + comma
}

fn estimated_encoded_value_len(value: &Value) -> usize {
    match value {
        Value::Null => 3,                          // size of nil
        Value::Boolean(_) => 5,                    // size of false
        Value::Integer(_) | Value::Float(_) => 16, // high estimates
        Value::Bytes(b) => b.len() + 2,            // base plus quotes
        Value::Timestamp(_) => 32,                 // aggressively high estimate
        Value::Array(arr) => {
            let base = arr
                .iter()
                .fold(0, |acc, x| acc + estimated_encoded_value_len(x));
            let commas = arr.len();
            let spaces = arr.len() * 2;
            let brackets = 2;
            base + commas + spaces + brackets
        }
        Value::Map(map) => {
            let base = map.iter().fold(0, |acc, (k, v)| {
                let key = k.len() + 2; // base plus quotes
                let val = estimated_encoded_value_len(v);
                acc + key + val
            });
            let brackets = 2;
            let spaces = map.len() * 2;
            let colons = map.len();

            base + brackets + spaces + colons
        }
    }
}

impl EncodedLength for LogMsg {
    /// Guess at the encoded length of a `LogMsg`
    ///
    /// This function returns the best guess -- plus a fudge factor -- of the
    /// encoded length of the [`LogMsg`] struct. It should be fairly accurate,
    /// if high. We explicitly avoid serializing the `LogMsg` to determine its
    /// completed byte weight to avoid blowing our throughput numbers.
    ///
    /// Length may be off if serialization is done compact, if strings have
    /// interior quoting, if very fine floating points are introduced etc.
    fn encoded_length(&self) -> usize {
        // "ddsource" : "THESTRING",
        let ddsource_len = kv_len(8, estimated_encoded_value_len(&self.ddsource));

        // "ddtags" : "THESTRING",
        let ddtags_len = kv_len(6, estimated_encoded_value_len(&self.ddtags));

        // "hostname" : "THESTRING",
        let hostname_len = kv_len(8, estimated_encoded_value_len(&self.hostname));

        // "message" : "THESTRING",
        let message_len = kv_len(7, estimated_encoded_value_len(&self.message));

        // "service" : "THESTRING",
        let service_len = kv_len(7, estimated_encoded_value_len(&self.service));

        let mut estimated_length =
            ddsource_len + ddtags_len + hostname_len + message_len + service_len;

        for (k, v) in self.extra.iter() {
            let key = k.len() + 2; // base plus quotes
            let spaces = 2;
            let colon = 1;
            let val = estimated_encoded_value_len(v);
            let comma = 1;

            estimated_length += key + spaces + colon + comma + val;
        }

        estimated_length
    }
}

#[async_trait::async_trait]
impl HttpSink for Service {
    type Input = PartitionInnerBuffer<LogMsg, ApiKey>;
    type Output = PartitionInnerBuffer<Vec<LogMsg>, ApiKey>;

    fn encode_event(&self, mut event: Event) -> Option<EncodedEvent<Self::Input>> {
        let log = event.as_mut_log();

        if let Some(message) = log.remove(self.log_schema_message_key) {
            log.insert_flat("message", message);
        }

        if let Some(timestamp) = log.remove(self.log_schema_timestamp_key) {
            log.insert_flat("date", timestamp);
        }

        if let Some(host) = log.remove(self.log_schema_host_key) {
            log.insert_flat("host", host);
        }

        self.encoding.apply_rules(&mut event);

        let (fields, metadata) = event.into_log().into_parts();
        let log_msg = LogMsg::try_from(fields).ok()?;
        let api_key = metadata
            .datadog_api_key()
            .as_ref()
            .unwrap_or(&self.default_api_key);

        Some(EncodedEvent {
            item: PartitionInnerBuffer::new(log_msg, Arc::clone(api_key)),
            metadata: Some(metadata),
        })
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Vec<u8>>> {
        let (events, api_key) = events.into_parts();

        let body: Vec<u8> = serde_json::to_vec(&events)?;
        // check the number of events to ignore health-check requests
        if !events.is_empty() {
            emit!(DatadogLogEventProcessed {
                byte_size: body.len(),
                count: events.len(),
            });
        }

        let request = Request::post(self.uri.clone())
            .header("Content-Type", "application/json")
            .header("DD-API-KEY", &api_key[..]);

        let (request, body) = match self.compression {
            Compression::None => (request, body),
            Compression::Gzip(level) => {
                let level = level.unwrap_or(GZIP_FAST);
                let mut encoder = GzEncoder::new(
                    Vec::with_capacity(body.len()),
                    flate2::Compression::new(level as u32),
                );

                encoder.write_all(&body)?;
                (
                    request.header("Content-Encoding", "gzip"),
                    encoder.finish()?,
                )
            }
        };

        request
            .header("Content-Length", body.len())
            .body(body)
            .map_err(Into::into)
    }
}
