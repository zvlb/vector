use crate::sinks::util::EncodedLength;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use vector_core::event::Value;

/// https://docs.datadoghq.com/api/latest/logs/#send-logs
#[derive(Serialize, Debug, Clone)]
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

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

    impl Arbitrary for LogMsg {
        fn arbitrary(g: &mut Gen) -> Self {
            LogMsg {
                ddsource: Value::arbitrary(g),
                ddtags: Value::arbitrary(g),
                hostname: Value::arbitrary(g),
                message: Value::arbitrary(g),
                service: Value::arbitrary(g),
                extra: BTreeMap::arbitrary(g),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let log_msg = self.clone();

            Box::new(
                log_msg
                    .extra
                    .shrink()
                    .map(move |extra| {
                        let mut log_msg = log_msg.clone();
                        log_msg.extra = extra;
                        log_msg
                    })
                    .flat_map(|log_msg| {
                        log_msg.service.shrink().map(move |svc| {
                            let mut log_msg = log_msg.clone();
                            log_msg.service = svc;
                            log_msg
                        })
                    })
                    .flat_map(|log_msg| {
                        log_msg.message.shrink().map(move |msg| {
                            let mut log_msg = log_msg.clone();
                            log_msg.message = msg;
                            log_msg
                        })
                    })
                    .flat_map(|log_msg| {
                        log_msg.hostname.shrink().map(move |hst| {
                            let mut log_msg = log_msg.clone();
                            log_msg.hostname = hst;
                            log_msg
                        })
                    })
                    .flat_map(|log_msg| {
                        log_msg.ddtags.shrink().map(move |tgs| {
                            let mut log_msg = log_msg.clone();
                            log_msg.ddtags = tgs;
                            log_msg
                        })
                    })
                    .flat_map(|log_msg| {
                        log_msg.ddsource.shrink().map(move |src| {
                            let mut log_msg = log_msg.clone();
                            log_msg.ddsource = src;
                            log_msg
                        })
                    }),
            )
        }
    }

    #[test]
    fn estimated_size_never_smaller() {
        // The estimated size of encoding should always be larger or equal to
        // the actual encoded size for every observed LogMsg.
        fn inner(msg: LogMsg) -> TestResult {
            let estimate = msg.encoded_length();
            let encoding: Vec<u8> = serde_json::to_vec(&msg).unwrap();

            debug_assert!(
                estimate >= encoding.len(),
                "{} >= {} | {}",
                estimate,
                encoding.len(),
                serde_json::to_string(&msg).unwrap()
            );
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(100_000)
            .max_tests(1_000_000)
            .quickcheck(inner as fn(LogMsg) -> TestResult);
    }
}
