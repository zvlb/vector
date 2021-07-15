use crate::ast::{Function, FunctionArgument};
use crate::parse_grok::Error as GrokRuntimeError;
use crate::parse_grok_rules::Error as GrokStaticError;
use parsing::key_value::Whitespace;
use parsing::{key_value, query_string, ruby_hash, value::Value};
use percent_encoding::percent_decode;
use regex::Regex;
use shared::btreemap;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::ops::Deref;
use std::string::ToString;
use strum_macros::Display;
use tracing::error;

#[derive(Debug, Display, Clone)]
pub enum GrokFilter {
    Integer,
    IntegerExt,
    Number,
    NumberExt,
    Boolean(Option<Regex>),
    NullIf(String),
    Scale(f64),
    Json,
    Rubyhash,
    Querystring,
    Lowercase,
    Uppercase,
    Decodeuricomponent,
    KeyValue(String, Option<Regex>, Vec<(char, char)>, Vec<String>),
}

impl TryFrom<&Function> for GrokFilter {
    type Error = GrokStaticError;

    fn try_from(f: &Function) -> Result<Self, Self::Error> {
        match f.name.as_str() {
            "boolean" => {
                if f.args.is_some() && !f.args.as_ref().unwrap().is_empty() {
                    if let FunctionArgument::Arg(Value::Bytes(ref bytes)) =
                        f.args.as_ref().unwrap()[0]
                    {
                        let pattern = String::from_utf8_lossy(bytes);
                        Ok(GrokFilter::Boolean(Some(
                            Regex::new(pattern.deref()).map_err(|error| {
                                error!(message = "Error compiling regex", path = %pattern, %error);
                                GrokStaticError::InvalidFunctionArguments(f.name.clone())
                            })?,
                        )))
                    } else {
                        Err(GrokStaticError::InvalidFunctionArguments(f.name.clone()))
                    }
                } else {
                    Ok(GrokFilter::Boolean(None))
                }
            }
            "nullIf" => {
                if f.args.is_some() && !f.args.as_ref().unwrap().is_empty() {
                    if let FunctionArgument::Arg(ref null_value) = f.args.as_ref().unwrap()[0] {
                        return Ok(GrokFilter::NullIf(null_value.to_string_lossy()));
                    }
                }
                Err(GrokStaticError::InvalidFunctionArguments(f.name.clone()))
            }
            "scale" => {
                if f.args.is_some() && !f.args.as_ref().unwrap().is_empty() {
                    let scale_factor = match f.args.as_ref().unwrap()[0] {
                        FunctionArgument::Arg(Value::Integer(scale_factor)) => scale_factor as f64,
                        FunctionArgument::Arg(Value::Float(scale_factor)) => scale_factor,
                        _ => return Err(GrokStaticError::InvalidFunctionArguments(f.name.clone())),
                    };
                    return Ok(GrokFilter::Scale(scale_factor));
                }
                Err(GrokStaticError::InvalidFunctionArguments(f.name.clone()))
            }
            "integer" => Ok(GrokFilter::Integer),
            "integerExt" => Ok(GrokFilter::IntegerExt),
            "number" => Ok(GrokFilter::Number),
            "numberExt" => Ok(GrokFilter::NumberExt),
            "json" => Ok(GrokFilter::Json),
            "rubyhash" => Ok(GrokFilter::Rubyhash),
            "querystring" => Ok(GrokFilter::Querystring),
            "lowercase" => Ok(GrokFilter::Lowercase),
            "uppercase" => Ok(GrokFilter::Uppercase),
            "decodeuricomponent" => Ok(GrokFilter::Decodeuricomponent),
            "keyvalue" => {
                let args_len = f.args.as_ref().map_or(0, |args| args.len());

                let key_value_delimiter = if args_len > 0 {
                    match f.args.as_ref().unwrap()[0] {
                        FunctionArgument::Arg(Value::Bytes(ref bytes)) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
                        _ => return Err(GrokStaticError::InvalidFunctionArguments(f.name.clone())),
                    }
                } else {
                    "=".to_string()
                };
                let value_re = if args_len > 1 {
                    match f.args.as_ref().unwrap()[1] {
                        FunctionArgument::Arg(Value::Bytes(ref bytes)) => {
                            let mut re_str = String::new();
                            re_str.push_str(r"^[\w.\-_@");
                            re_str.push_str(String::from_utf8_lossy(bytes).deref());
                            re_str.push_str("]+");
                            Regex::new(re_str.as_str()).map_err(|_e| {
                                GrokStaticError::InvalidFunctionArguments(f.name.clone())
                            })?
                        }
                        _ => return Err(GrokStaticError::InvalidFunctionArguments(f.name.clone())),
                    }
                } else {
                    Regex::new(r"^[\w.\-_@]+").unwrap()
                };

                let quotes = if args_len > 2 {
                    match f.args.as_ref().unwrap()[2] {
                        FunctionArgument::Arg(Value::Bytes(ref bytes)) => {
                            let quotes = String::from_utf8_lossy(bytes);
                            if quotes.len() == 2 {
                                let mut chars = quotes.chars();
                                vec![(
                                    chars.next().expect("open quote"),
                                    chars.next().expect("closing quote"),
                                )]
                            } else if quotes.is_empty() {
                                // default
                                vec![('"', '"'), ('\'', '\''), ('<', '>')]
                            } else {
                                return Err(GrokStaticError::InvalidFunctionArguments(
                                    f.name.clone(),
                                ));
                            }
                        }
                        _ => return Err(GrokStaticError::InvalidFunctionArguments(f.name.clone())),
                    }
                } else {
                    vec![('"', '"'), ('\'', '\''), ('<', '>')]
                };

                let field_delimiters = if args_len > 3 {
                    match f.args.as_ref().unwrap()[3] {
                        FunctionArgument::Arg(Value::Bytes(ref bytes)) => {
                            vec![String::from_utf8_lossy(bytes).to_string()]
                        }
                        _ => return Err(GrokStaticError::InvalidFunctionArguments(f.name.clone())),
                    }
                } else {
                    vec![" ".into(), ",".into(), ";".into()]
                };
                Ok(GrokFilter::KeyValue(
                    key_value_delimiter,
                    Some(value_re),
                    quotes,
                    field_delimiters,
                ))
            }
            _ => Err(GrokStaticError::UnknownFilter(f.name.clone())),
        }
    }
}

pub fn apply_filter(value: &Value, filter: &GrokFilter) -> Result<Value, GrokRuntimeError> {
    match filter {
        GrokFilter::Integer => match value {
            Value::Bytes(v) => Ok(String::from_utf8_lossy(v)
                .parse::<i64>()
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                })?
                .into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::IntegerExt => match value {
            Value::Bytes(v) => Ok(String::from_utf8_lossy(v)
                .parse::<f64>()
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                })
                .map(|f| (f as i64).into())
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                })?),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Number | GrokFilter::NumberExt => match value {
            Value::Bytes(v) => Ok(String::from_utf8_lossy(v)
                .parse::<f64>()
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                })?
                .into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Boolean(true_pattern) => match value {
            Value::Bytes(v) => {
                let is_true = match true_pattern {
                    Some(true_pattern) => {
                        true_pattern.is_match(String::from_utf8_lossy(v).as_ref())
                    }
                    None => "true".eq_ignore_ascii_case(String::from_utf8_lossy(v).as_ref()),
                };
                Ok(is_true.into())
            }
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::NullIf(null_value) => match value {
            Value::Bytes(_) => {
                if value.to_string_lossy() == *null_value {
                    Ok(Value::Null)
                } else {
                    Ok(value.to_owned())
                }
            }
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Scale(scale_factor) => match value {
            Value::Integer(v) => Ok(Value::Float((*v as f64) * scale_factor)),
            Value::Float(v) => Ok(Value::Float(*v * scale_factor)),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Json => match value {
            Value::Bytes(bytes) => serde_json::from_slice::<'_, serde_json::Value>(bytes.as_ref())
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                })
                .map(|v| v.into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Rubyhash => match value {
            Value::Bytes(bytes) => ruby_hash::parse(String::from_utf8_lossy(&bytes).as_ref())
                .map_err(|_e| {
                    GrokRuntimeError::FailedToApplyFilter(
                        filter.to_string(),
                        value.to_string_lossy(),
                    )
                }),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Querystring => match value {
            Value::Bytes(bytes) => query_string::parse(bytes).map_err(|_e| {
                GrokRuntimeError::FailedToApplyFilter(filter.to_string(), value.to_string_lossy())
            }),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Lowercase => match value {
            Value::Bytes(bytes) => Ok(String::from_utf8_lossy(&bytes).to_lowercase().into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Uppercase => match value {
            Value::Bytes(bytes) => Ok(String::from_utf8_lossy(&bytes).to_uppercase().into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::Decodeuricomponent => match value {
            Value::Bytes(bytes) => Ok(percent_decode(bytes).decode_utf8_lossy().to_string().into()),
            _ => Err(GrokRuntimeError::FailedToApplyFilter(
                filter.to_string(),
                value.to_string_lossy(),
            )),
        },
        GrokFilter::KeyValue(key_value_delimiter, value_re, quotes, field_delimiters) => {
            match value {
                Value::Bytes(bytes) => Ok(Value::from_iter::<Vec<(String, Value)>>(
                    key_value::parse(
                        String::from_utf8_lossy(bytes).as_ref(),
                        key_value_delimiter,
                        &field_delimiters
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<&str>>(),
                        quotes,
                        value_re.as_ref(),
                        Whitespace::Strict,
                        false,
                    )
                    .map_err(|_e| {
                        GrokRuntimeError::FailedToApplyFilter(
                            filter.to_string(),
                            value.to_string_lossy(),
                        )
                    })?
                    .iter()
                    .filter(|(_k, v)| !v.is_empty())
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                )),
                _ => Err(GrokRuntimeError::FailedToApplyFilter(
                    filter.to_string(),
                    value.to_string_lossy(),
                )),
            }
        }
    }
}
