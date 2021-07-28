use crate::sinks::util::EncodedLength;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use vector_core::event::Value;

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
