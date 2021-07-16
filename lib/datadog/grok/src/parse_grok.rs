use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;

use lookup::LookupBuf;
use parsing::value::Value;
use shared::btreemap;

use crate::grok_filter::apply_filter;
use crate::insert_field::insert_field;
use crate::parse_grok_rules::GrokRule;
use tracing::warn;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("failed to apply filter '{}' to '{}'", .0, .1)]
    FailedToApplyFilter(String, String),
    #[error("value does not match any rule")]
    NoMatch,
}

pub fn parse_grok(source_field: &str, grok_rules: &[GrokRule]) -> Result<Value, Error> {
    grok_rules
        .iter()
        .fold_while(Err(Error::NoMatch), |_, rule| {
            let result = apply_grok_rule(source_field, rule);
            if let Err(Error::NoMatch) = result {
                Continue(result)
            } else {
                Done(result)
            }
        })
        .into_inner()
}

fn apply_grok_rule(source: &str, grok_rule: &GrokRule) -> Result<Value, Error> {
    let mut parsed = Value::from(btreemap! {});

    if let Some(ref matches) = grok_rule.pattern.match_against(source) {
        for (name, value) in matches.iter() {
            let path: LookupBuf = if name == "." {
                LookupBuf::root()
            } else {
                name.parse().expect("path should always be valid")
            };

            let mut value = Some(Value::from(value));

            // apply filters
            if let Some(filters) = grok_rule.filters.get(&path) {
                filters.iter().for_each(|filter| {
                    match apply_filter(&value.as_ref().unwrap(), filter) {
                        Ok(v) => value = Some(v),
                        Err(error) => {
                            warn!(message = "Error applying filter", path = %path, filter = %filter, %error);
                            value = None;
                        }
                    }
                });
            };

            if let Some(value) = value {
                insert_field(&mut parsed, path.clone(), value).unwrap_or_else(
                    |error| warn!(message = "Error updating field value", path = %path, %error),
                );
            }
        }

        Ok(parsed)
    } else {
        Err(Error::NoMatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_grok_rules::parse_grok_rules;

    #[test]
    fn parses_simple_grok() {
        let rules = parse_grok_rules(
            &[],
            &[
                "simple %{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"
                    .to_string(),
            ],
        )
        .expect("should parse rules");
        let parsed = parse_grok("2020-10-02T23:22:12.223222Z info Hello world", &rules).unwrap();

        assert_eq!(
            parsed,
            Value::from(btreemap! {
                "timestamp" => "2020-10-02T23:22:12.223222Z",
                "level" => "info",
                "message" => "Hello world"
            })
        );
    }

    #[test]
    fn parses_complex_grok() {
        let rules = parse_grok_rules(
            // helper rules
            &[
                r#"_auth %{notSpace:http.auth:nullIf("-")}"#.to_string(),
                r#"_bytes_written %{integer:network.bytes_written}"#.to_string(),
                r#"_client_ip %{ipOrHost:network.client.ip}"#.to_string(),
                r#"_version HTTP\/(?<http.version>\d+\.\d+)"#.to_string(),
                r#"_url %{notSpace:http.url}"#.to_string(),
                r#"_ident %{notSpace:http.ident}"#.to_string(),
                r#"_user_agent %{regex("[^\\\"]*"):http.useragent}"#.to_string(),
                r#"_referer %{notSpace:http.referer}"#.to_string(),
                r#"_status_code %{integer:http.status_code}"#.to_string(),
                r#"_method %{word:http.method}"#.to_string(),
                r#"_date_access %{date("dd/MMM/yyyy:HH:mm:ss Z"):date_access}"#.to_string(),
                r#"_x_forwarded_for %{regex("[^\\\"]*"):http._x_forwarded_for:nullIf("-")}"#.to_string()],
            // parsing rules
            &[
                r#"access.common %{_client_ip} %{_ident} %{_auth} \[%{_date_access}\] "(?>%{_method} |)%{_url}(?> %{_version}|)" %{_status_code} (?>%{_bytes_written}|-)"#.to_string(),
                r#"access.combined %{access.common} (%{number:duration:scale(1000000000)} )?"%{_referer}" "%{_user_agent}"( "%{_x_forwarded_for}")?.*"#.to_string()
            ]).expect("should parse rules");
        let parsed = parse_grok(r##"127.0.0.1 - frank [13/Jul/2016:10:55:36 +0000] "GET /apache_pb.gif HTTP/1.0" 200 2326 0.202 "http://www.perdu.com/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36" "-""##, &rules).unwrap();

        assert_eq!(
            parsed,
            Value::from(btreemap! {
                "date_access" => "13/Jul/2016:10:55:36 +0000",
                "duration" => 202000000.0,
                "http" => btreemap! {
                    "auth" => "frank",
                    "ident" => "-",
                    "method" => "GET",
                    "status_code" => 200,
                    "url" => "/apache_pb.gif",
                    "version" => "1.0",
                    "referer" => "http://www.perdu.com/",
                    "useragent" => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
                    "_x_forwarded_for" => Value::Null,
                },
                "network" => btreemap! {
                    "bytes_written" => 2326,
                    "client" => btreemap! {
                        "ip" => "127.0.0.1"
                    }
                }
            })
        );
    }

    #[test]
    fn supports_matchers() {
        test_grok_pattern(vec![
            (
                "%{numberStr:field}",
                "-1.2",
                Ok(Value::Bytes("-1.2".into())),
            ),
            ("%{number:field}", "-1.2", Ok(Value::Float(-1.2_f64))),
            ("%{number:field}", "-1", Ok(Value::Float(-1_f64))),
            (
                "%{numberExt:field}",
                "-1234e+3",
                Ok(Value::Float(-1234e+3_f64)),
            ),
            ("%{numberExt:field}", ".1e+3", Ok(Value::Float(0.1e+3_f64))),
            ("%{integer:field}", "-2", Ok(Value::Integer(-2))),
            ("%{integerExt:field}", "+2", Ok(Value::Integer(2))),
            ("%{integerExt:field}", "-2", Ok(Value::Integer(-2))),
            ("%{integerExt:field}", "-1e+2", Ok(Value::Integer(-100))),
            ("%{integerExt:field}", "1234.1e+5", Err(Error::NoMatch)),
            ("%{boolean:field}", "tRue", Ok(Value::Boolean(true))), // true/false are default values(case-insensitive)
            ("%{boolean:field}", "False", Ok(Value::Boolean(false))),
            (
                r#"%{boolean("ok", "no"):field}"#,
                "ok",
                Ok(Value::Boolean(true)),
            ),
            (
                r#"%{boolean("ok", "no"):field}"#,
                "no",
                Ok(Value::Boolean(false)),
            ),
            (r#"%{boolean("ok", "no"):field}"#, "No", Err(Error::NoMatch)),
            (
                "%{doubleQuotedString:field}",
                r#""test  ""#,
                Ok(Value::Bytes(r#""test  ""#.into())),
            ),
        ]);
    }

    #[test]
    fn supports_filters_without_fields() {
        // if the value, after filters applied, is a map then merge it at the root level
        test_grok_pattern_without_field(vec![(
            "%{notSpace:standalone_field} '%{data::json}' '%{data::json}'",
            r#"value1 '{ "json_field1": "value2" }' '{ "json_field2": "value3" }'"#,
            Ok(Value::from(btreemap! {
                "standalone_field" => Value::Bytes("value1".into()),
                "json_field1" => Value::Bytes("value2".into()),
                "json_field2" => Value::Bytes("value3".into())
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{notSpace:standalone_field} '%{data:nested.json:json}' '%{data:nested.json:json}'",
            r#"value1 '{ "json_field1": "value2" }' '{ "json_field2": "value3" }'"#,
            Ok(Value::from(btreemap! {
                "standalone_field" => Value::Bytes("value1".into()),
                "nested" => btreemap! {
                    "json" =>  btreemap! {
                        "json_field1" => Value::Bytes("value2".into()),
                        "json_field2" => Value::Bytes("value3".into())
                    }
                }
            })),
        )]);
        // otherwise ignore it
        test_grok_pattern_without_field(vec![(
            "%{notSpace:standalone_field} %{data::integer}",
            r#"value1 1"#,
            Ok(Value::from(btreemap! {
                "standalone_field" => Value::Bytes("value1".into()),
            })),
        )]);
        // empty map if fails
        test_grok_pattern_without_field(vec![(
            "%{data::json}",
            r#"not a json"#,
            Ok(Value::from(btreemap! {})),
        )]);
    }

    #[test]
    fn ignores_field_if_filter_fails() {
        // empty map for filters like json
        test_grok_pattern_without_field(vec![(
            "%{notSpace:field1:integer} %{data:field2:json}",
            r#"not_a_number not a json"#,
            Ok(Value::from(btreemap! {})),
        )]);
    }

    #[test]
    fn supports_filters() {
        test_grok_pattern(vec![
            ("%{data:field:number}", "1.0", Ok(Value::Float(1.0_f64))),
            ("%{data:field:integer}", "1", Ok(Value::Integer(1))),
            (r#"%{data:field:nullIf("-")}"#, "-", Ok(Value::Null)),
            (
                r#"%{data:field:nullIf("-")}"#,
                "abc",
                Ok(Value::Bytes("abc".into())),
            ),
            ("%{data:field:boolean}", "tRue", Ok(Value::Boolean(true))),
            ("%{data:field:boolean}", "false", Ok(Value::Boolean(false))),
            (
                "%{data:field:json}",
                r#"{ "bool_field": true, "array": ["abc"] }"#,
                Ok(Value::from(
                    btreemap! { "bool_field" => Value::Boolean(true), "array" => Value::Array(vec!["abc".into()])},
                )),
            ),
            (
                "%{data:field:rubyhash}",
                r#"{ "test" => "value", "testNum" => 0.2, "testObj" => { "testBool" => true } }"#,
                Ok(Value::from(
                    btreemap! { "test" => "value", "testNum" => 0.2, "testObj" => Value::from(btreemap! {"testBool" => true})},
                )),
            ),
            (
                "%{data:field:querystring}",
                "?productId=superproduct&promotionCode=superpromo",
                Ok(Value::from(
                    btreemap! { "productId" => "superproduct", "promotionCode" => "superpromo"},
                )),
            ),
            (
                "%{data:field:lowercase}",
                "aBC",
                Ok(Value::Bytes("abc".into())),
            ),
            (
                "%{data:field:uppercase}",
                "Abc",
                Ok(Value::Bytes("ABC".into())),
            ),
            (
                "%{data:field:decodeuricomponent}",
                "%2Fservice%2Ftest",
                Ok(Value::Bytes("/service/test".into())),
            ),
            ("%{integer:field:scale(10)}", "1", Ok(Value::Float(10.0))),
            ("%{number:field:scale(0.5)}", "10.0", Ok(Value::Float(5.0))),
        ]);
    }

    #[test]
    fn parses_key_value() {
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key=valueStr",
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key=<valueStr>",
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            r#""key"="valueStr""#,
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            r#"'key'='valueStr'"#,
            Ok(Value::from(btreemap! {
               "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            r#"<key>=<valueStr>"#,
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue(":")}"#,
            r#"key:valueStr"#,
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue(":", "/")}"#,
            r#"key:"/valueStr""#,
            Ok(Value::from(btreemap! {
                "key" => "/valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue(":", "/")}"#,
            r#"/key:/valueStr"#,
            Ok(Value::from(btreemap! {
                "/key" => "/valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue(":=", "", "{}")}"#,
            r#"key:={valueStr}"#,
            Ok(Value::from(btreemap! {
                "key" => "valueStr"
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue("=", "", "", "|")}"#,
            r#"key1=value1|key2=value2"#,
            Ok(Value::from(btreemap! {
                "key1" => "value1",
                "key2" => "value2",
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue("=", "", "", "|")}"#,
            r#"key1="value1"|key2="value2""#,
            Ok(Value::from(btreemap! {
                "key1" => "value1",
                "key2" => "value2",
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue(":=","","<>")}"#,
            r#"key1:=valueStr key2:=</valueStr2> key3:="valueStr3""#,
            Ok(Value::from(btreemap! {
                "key1" => "valueStr",
                "key2" => "/valueStr2",
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue}"#,
            r#"key1=value1,key2=value2"#,
            Ok(Value::from(btreemap! {
                "key1" => "value1",
                "key2" => "value2",
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            r#"%{data::keyvalue}"#,
            r#"key1=value1;key2=value2"#,
            Ok(Value::from(btreemap! {
                "key1" => "value1",
                "key2" => "value2",
            })),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key:=valueStr",
            Ok(Value::from(btreemap! {})),
        )]);
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key:=valueStr",
            Ok(Value::from(btreemap! {})),
        )]);
        // empty key or null
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key1= key2=null key3=value3",
            Ok(Value::from(btreemap! {
                "key3" => "value3"
            })),
        )]);
        // empty value or null - comma-separated
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "key1=,key2=null,key3= ,key4=value4",
            Ok(Value::from(btreemap! {
                "key4" => "value4"
            })),
        )]);
        // empty key
        test_grok_pattern_without_field(vec![(
            "%{data::keyvalue}",
            "=,=value",
            Ok(Value::from(btreemap! {})),
        )]);
    }

    fn test_grok_pattern_without_field(tests: Vec<(&str, &str, Result<Value, Error>)>) {
        for (filter, k, v) in tests {
            let rules = parse_grok_rules(&[], &[format!(r#"test {}"#, filter)])
                .expect("should parse rules");
            let parsed = parse_grok(k, &rules);

            if v.is_ok() {
                assert_eq!(parsed.unwrap(), v.unwrap());
            } else {
                assert_eq!(parsed, v);
            }
        }
    }

    fn test_grok_pattern(tests: Vec<(&str, &str, Result<Value, Error>)>) {
        for (filter, k, v) in tests {
            let rules = parse_grok_rules(&[], &[format!(r#"test {}"#, filter)])
                .expect("should parse rules");
            let parsed = parse_grok(k, &rules);

            if v.is_ok() {
                assert_eq!(
                    parsed.unwrap(),
                    Value::from(btreemap! {
                        "field" =>  v.unwrap(),
                    })
                );
            } else {
                assert_eq!(parsed, v);
            }
        }
    }

    #[test]
    fn fails_on_invalid_grok_format() {
        assert_eq!(
            parse_grok_rules(&[], &["%{data}".to_string()])
                .unwrap_err()
                .to_string(),
            "failed to parse grok expression '%{data}': format must be: 'ruleName definition'"
        );
    }

    #[test]
    fn fails_on_unknown_pattern_definition() {
        assert_eq!(
            parse_grok_rules(&[], &["test %{unknown}".to_string()])
                .unwrap_err()
                .to_string(),
            r#"failed to parse grok expression '^%{unknown}$': The given pattern definition name "unknown" could not be found in the definition map"#
        );
    }

    #[test]
    fn fails_on_unknown_filter() {
        assert_eq!(
            parse_grok_rules(&[], &["test %{data:field:unknownFilter}".to_string()])
                .unwrap_err()
                .to_string(),
            r#"unknown filter 'unknownFilter'"#
        );
    }

    #[test]
    fn fails_on_invalid_matcher_parameter() {
        assert_eq!(
            parse_grok_rules(&[], &["test_rule %{regex(1):field}".to_string()])
                .unwrap_err()
                .to_string(),
            r#"invalid arguments for the function 'regex'"#
        );
    }

    #[test]
    fn fails_on_invalid_filter_parameter() {
        assert_eq!(
            parse_grok_rules(&[], &["test_rule %{data:field:scale()}".to_string()])
                .unwrap_err()
                .to_string(),
            r#"invalid arguments for the function 'scale'"#
        );
    }

    #[test]
    fn fails_on_no_match() {
        let rules = parse_grok_rules(
            &[],
            &[
                "test_rule %{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"
                    .to_string(),
            ],
        )
        .expect("should parse rules");
        let error = parse_grok("an ungrokkable message", &rules).unwrap_err();

        assert_eq!(error, Error::NoMatch);
    }

    #[test]
    fn appends_to_the_same_field() {
        let rules = parse_grok_rules(
            &[],
            &[
                r#"simple %{integer:nested.field} %{notSpace:nested.field:uppercase} %{notSpace:nested.field:nullIf("-")}"#
                    .to_string(),
            ],
        )
            .expect("should parse rules");
        let parsed = parse_grok("1 info -", &rules).unwrap();

        assert_eq!(
            parsed,
            Value::from(btreemap! {
                "nested" => btreemap! {
                   "field" =>  Value::Array(vec![1.into(), "INFO".into(), Value::Null]),
                },
            })
        );
    }
}
