use parsing::key_value::{parse, Whitespace};
use std::{iter::FromIterator, str::FromStr};
use vrl::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct ParseKeyValue;

impl Function for ParseKeyValue {
    fn identifier(&self) -> &'static str {
        "parse_key_value"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "key_value_delimiter",
                kind: kind::ANY,
                required: false,
            },
            Parameter {
                keyword: "field_delimiter",
                kind: kind::ANY,
                required: false,
            },
            Parameter {
                keyword: "whitespace",
                kind: kind::BYTES,
                required: false,
            },
            Parameter {
                keyword: "accept_standalone_key",
                kind: kind::BOOLEAN,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "simple key value",
                source: r#"parse_key_value!("zork=zook zonk=nork")"#,
                result: Ok(r#"{"zork": "zook", "zonk": "nork"}"#),
            },
            Example {
                title: "custom delimiters",
                source: r#"parse_key_value!(s'zork: zoog, nonk: "nink nork"', key_value_delimiter: ":", field_delimiter: ",")"#,
                result: Ok(r#"{"zork": "zoog", "nonk": "nink nork"}"#),
            },
            Example {
                title: "strict whitespace",
                source: r#"parse_key_value!(s'app=my-app ip=1.2.3.4 user= msg=hello-world', whitespace: "strict")"#,
                result: Ok(
                    r#"{"app": "my-app", "ip": "1.2.3.4", "user": "", "msg": "hello-world"}"#,
                ),
            },
            Example {
                title: "standalone key",
                source: r#"parse_key_value!(s'foo=bar foobar', whitespace: "strict")"#,
                result: Ok(r#"{"foo": "bar", "foobar": true}"#),
            },
        ]
    }

    fn compile(&self, mut arguments: ArgumentList) -> Compiled {
        let value = arguments.required("value");

        let key_value_delimiter = arguments
            .optional("key_value_delimiter")
            .unwrap_or_else(|| expr!("="));

        let field_delimiter = arguments
            .optional("field_delimiter")
            .unwrap_or_else(|| expr!(" "));

        let whitespace = arguments
            .optional_enum(
                "whitespace",
                &Whitespace::all_value()
                    .iter()
                    .map(|v| v.to_owned().into())
                    .collect::<Vec<Value>>(),
            )?
            .map(|s| {
                Whitespace::from_str(&s.try_bytes_utf8_lossy().expect("whitespace not bytes"))
                    .expect("validated enum")
            })
            .unwrap_or_default();

        let standalone_key = arguments
            .optional("accept_standalone_key")
            .unwrap_or_else(|| expr!(true));

        Ok(Box::new(ParseKeyValueFn {
            value,
            key_value_delimiter,
            field_delimiter,
            whitespace,
            standalone_key,
        }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ParseKeyValueFn {
    pub(crate) value: Box<dyn Expression>,
    pub(crate) key_value_delimiter: Box<dyn Expression>,
    pub(crate) field_delimiter: Box<dyn Expression>,
    pub(crate) whitespace: Whitespace,
    pub(crate) standalone_key: Box<dyn Expression>,
}

impl Expression for ParseKeyValueFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let bytes = value.try_bytes_utf8_lossy()?;

        let value = self.key_value_delimiter.resolve(ctx)?;
        let key_value_delimiter = value.try_bytes_utf8_lossy()?;

        let value = self.field_delimiter.resolve(ctx)?;
        let field_delimiter = value.try_bytes_utf8_lossy()?;

        let standalone_key = self.standalone_key.resolve(ctx)?.try_boolean()?;

        let values = parse(
            &bytes,
            &key_value_delimiter,
            &[&field_delimiter],
            &[('"', '"')],
            None,
            self.whitespace,
            standalone_key,
        )?
        .iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned().into()))
        .collect::<Vec<(String, Value)>>();

        Ok(Value::from_iter(values))
    }

    fn type_def(&self, _: &state::Compiler) -> TypeDef {
        TypeDef::new().fallible().object::<(), Kind>(map! {
            (): Kind::all()
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    test_function![
        parse_key_value => ParseKeyValue;

        default {
            args: func_args! [
                value: r#"at=info method=GET path=/ host=myapp.herokuapp.com request_id=8601b555-6a83-4c12-8269-97c8e32cdb22 fwd="204.204.204.204" dyno=web.1 connect=1ms service=18ms status=200 bytes=13 tls_version=tls1.1 protocol=http"#,
            ],
            want: Ok(value!({at: "info",
                             method: "GET",
                             path: "/",
                             host: "myapp.herokuapp.com",
                             request_id: "8601b555-6a83-4c12-8269-97c8e32cdb22",
                             fwd: "204.204.204.204",
                             dyno: "web.1",
                             connect: "1ms",
                             service: "18ms",
                             status: "200",
                             bytes: "13",
                             tls_version: "tls1.1",
                             protocol: "http"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        logfmt {
            args: func_args! [
                value: r#"level=info msg="Stopping all fetchers" tag=stopping_fetchers id=ConsumerFetcherManager-1382721708341 module=kafka.consumer.ConsumerFetcherManager"#
            ],
            want: Ok(value!({level: "info",
                             msg: "Stopping all fetchers",
                             tag: "stopping_fetchers",
                             id: "ConsumerFetcherManager-1382721708341",
                             module: "kafka.consumer.ConsumerFetcherManager"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        // From https://github.com/timberio/vector/issues/5347
        real_case {
            args: func_args! [
                value: r#"SerialNum=100018002000001906146520 GenTime="2019-10-24 14:25:03" SrcIP=10.10.254.2 DstIP=10.10.254.7 Protocol=UDP SrcPort=137 DstPort=137 PolicyID=3 Action=PERMIT Content="Session Backout""#
            ],
            want: Ok(value!({SerialNum: "100018002000001906146520",
                             GenTime: "2019-10-24 14:25:03",
                             SrcIP: "10.10.254.2",
                             DstIP: "10.10.254.7",
                             Protocol: "UDP",
                             SrcPort: "137",
                             DstPort: "137",
                             PolicyID: "3",
                             Action: "PERMIT",
                             Content: "Session Backout"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        strict {
            args: func_args! [
                value: r#"foo= bar= tar=data"#,
                whitespace: "strict"
            ],
            want: Ok(value!({foo: "",
                             bar: "",
                             tar: "data"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        spaces {
            args: func_args! [
                value: r#""zork one" : "zoog\"zink\"zork"        nonk          : nink"#,
                key_value_delimiter: ":",
            ],
            want: Ok(value!({"zork one": r#"zoog\"zink\"zork"#,
                             nonk: "nink"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        delimited {
            args: func_args! [
                value: r#""zork one":"zoog\"zink\"zork", nonk:nink"#,
                key_value_delimiter: ":",
                field_delimiter: ",",
            ],
            want: Ok(value!({"zork one": r#"zoog\"zink\"zork"#,
                             nonk: "nink"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        delimited_with_spaces {
            args: func_args! [
                value: r#""zork one" : "zoog\"zink\"zork"  ,      nonk          : nink"#,
                key_value_delimiter: ":",
                field_delimiter: ",",
            ],
            want: Ok(value!({"zork one": r#"zoog\"zink\"zork"#,
                             nonk: "nink"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        multiple_chars {
            args: func_args! [
                value: r#""zork one" -- "zoog\"zink\"zork"  ||    nonk          -- nink"#,
                key_value_delimiter: "--",
                field_delimiter: "||",
            ],
            want: Ok(value!({"zork one": r#"zoog\"zink\"zork"#,
                             nonk: "nink"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        error {
            args: func_args! [
                value: r#"I am not a valid line."#,
                key_value_delimiter: "--",
                field_delimiter: "||",
                accept_standalone_key: false,
            ],
            want: Err("0: at line 1, in Tag:\nI am not a valid line.\n                      ^\n\n1: at line 1, in ManyMN:\nI am not a valid line.\n                      ^\n\n"),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        // The following case demonstrates a scenario that could potentially be considered an
        // error, but isn't. It is possible that we are missing a separator here (between nink and
        // norgle), but it parses it successfully and just assumes all the text after the
        // key_value_delimiter is the value since there is no terminator to stop the parsing.
        missing_separator {
            args: func_args! [
                value: r#"zork: zoog, nonk: nink norgle: noog"#,
                key_value_delimiter: ":",
                field_delimiter: ",",
            ],
            want: Ok(value!({zork: r#"zoog"#,
                             nonk: "nink norgle: noog"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        // If the value field is delimited and we miss the separator,
        // the following field is consumed by the current one.
        missing_separator_delimited {
            args: func_args! [
                value: r#"zork: zoog, nonk: "nink" norgle: noog"#,
                key_value_delimiter: ":",
                field_delimiter: ",",
            ],
            want: Ok(value!({zork: "zoog",
                             nonk: r#""nink" norgle: noog"#})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        multi_line_with_quotes {
            args: func_args! [
                value: "To: tom\ntest: \"tom\" test",
                key_value_delimiter: ":",
                field_delimiter: "\n",
            ],
            want: Ok(value!({"To": "tom",
                             "test": "\"tom\" test"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }

        multi_line_with_quotes_spaces {
            args: func_args! [
                value: "To: tom\ntest: \"tom test\"  ",
                key_value_delimiter: ":",
                field_delimiter: "\n",
            ],
            want: Ok(value!({"To": "tom",
                             "test": "tom test"})),
            tdef: TypeDef::new().fallible().object::<(), Kind>(map! {
                (): Kind::all()
            }),
        }
    ];
}
