use crate::value::Value;
use nom::{
    self,
    branch::alt,
    bytes::complete::{escaped, tag, take_until, take_while1},
    character::complete::{char, satisfy, space0},
    combinator::{eof, map, opt, peek, rest, verify},
    error::{ContextError, ParseError, VerboseError},
    multi::{many1, many_m_n, separated_list1},
    regexp::str::re_find,
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};
use regex::Regex;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Whitespace {
    Strict,
    Lenient,
}

impl Whitespace {
    pub fn all_value() -> Vec<Value> {
        use Whitespace::*;

        vec![Strict, Lenient]
            .into_iter()
            .map(|u| u.as_str().into())
            .collect::<Vec<_>>()
    }

    const fn as_str(self) -> &'static str {
        use Whitespace::*;

        match self {
            Strict => "strict",
            Lenient => "lenient",
        }
    }
}

impl Default for Whitespace {
    fn default() -> Self {
        Self::Lenient
    }
}

impl FromStr for Whitespace {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use Whitespace::*;

        match s {
            "strict" => Ok(Strict),
            "lenient" => Ok(Lenient),
            _ => Err("unknown whitespace variant"),
        }
    }
}

pub fn parse<'a>(
    input: &'a str,
    key_value_delimiter: &'a str,
    field_delimiters: &'a [&'a str],
    quotes: &'a [(char, char)],
    value_re: Option<&Regex>,
    whitespace: Whitespace,
    standalone_key: bool,
) -> Result<Vec<(String, Value)>, String> {
    let (rest, result) = parse_line(
        input,
        key_value_delimiter,
        field_delimiters,
        quotes,
        value_re,
        whitespace,
        standalone_key,
    )
    .map_err(|e| match e {
        nom::Err::Error(e) | nom::Err::Failure(e) => {
            // Create a descriptive error message if possible.
            nom::error::convert_error(input, e)
        }
        _ => format!("{}", e),
    })?;

    if rest.trim().is_empty() {
        Ok(result)
    } else {
        Err("could not parse whole line successfully".into())
    }
}

/// Parse the line as a separated list of key value pairs.
fn parse_line<'a>(
    input: &'a str,
    key_value_delimiter: &'a str,
    field_delimiters: &'a [&'a str],
    quotes: &'a [(char, char)],
    value_re: Option<&'a Regex>,
    whitespace: Whitespace,
    standalone_key: bool,
) -> IResult<&'a str, Vec<(String, Value)>, VerboseError<&'a str>> {
    let mut last_result = None;
    for &field_delimiter in field_delimiters {
        match separated_list1(
            parse_field_delimiter(field_delimiter),
            parse_key_value(
                key_value_delimiter,
                field_delimiter,
                quotes,
                value_re,
                whitespace,
                standalone_key,
            ),
        )(input)
        {
            Ok((rest, v)) if rest.trim().is_empty() => return Ok((rest, v)),
            res => last_result = Some(res), // continue
        }
    }
    last_result.unwrap()
}

/// Parses the field_delimiter between the key/value pairs.
/// If the field_delimiter is a space, we parse as many as we can,
/// If it is not a space eat any whitespace before our field_delimiter as well as the field_delimiter.
fn parse_field_delimiter<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    field_delimiter: &'a str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| {
        if field_delimiter == " " {
            map(many1(tag(field_delimiter)), |_| " ")(input)
        } else {
            preceded(space0, tag(field_delimiter))(input)
        }
    }
}

/// Parse a single `key=value` tuple.
/// Always accepts `key=`
/// Accept standalone `key` if `standalone_key` is `true`
fn parse_key_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    key_value_delimiter: &'a str,
    field_delimiter: &'a str,
    quotes: &'a [(char, char)],
    non_quoted_re: Option<&'a Regex>,
    whitespace: Whitespace,
    standalone_key: bool,
) -> impl Fn(&'a str) -> IResult<&'a str, (String, Value), E> {
    move |input| {
        map(
            |input| match whitespace {
                Whitespace::Strict => tuple((
                    alt((
                        verify(
                            preceded(
                                space0,
                                parse_key(key_value_delimiter, quotes, non_quoted_re),
                            ),
                            |s: &str| !(standalone_key && s.contains(field_delimiter)),
                        ),
                        preceded(space0, parse_key(field_delimiter, quotes, non_quoted_re)),
                    )),
                    many_m_n(!standalone_key as usize, 1, tag(key_value_delimiter)),
                    parse_value(field_delimiter, quotes, non_quoted_re),
                ))(input),
                Whitespace::Lenient => tuple((
                    alt((
                        verify(
                            preceded(
                                space0,
                                parse_key(key_value_delimiter, quotes, non_quoted_re),
                            ),
                            |s: &str| !(standalone_key && s.contains(field_delimiter)),
                        ),
                        preceded(space0, parse_key(field_delimiter, quotes, non_quoted_re)),
                    )),
                    many_m_n(
                        !standalone_key as usize,
                        1,
                        delimited(space0, tag(key_value_delimiter), space0),
                    ),
                    parse_value(field_delimiter, quotes, non_quoted_re),
                ))(input),
            },
            |(field, sep, value): (&str, Vec<&str>, Value)| {
                if sep.len() == 1 {
                    (field.to_string(), value)
                } else {
                    (field.to_string(), Value::Boolean(true))
                }
            },
        )(input)
    }
}

/// Parses a string delimited by the given character.
/// Can be escaped using `\`.
/// The terminator indicates the character that should follow the delimited field.
/// This captures the situation where a field is not actually delimited but starts with
/// some text that appears delimited:
/// `field: "some kind" of value`
/// We want to error in this situation rather than return a partially parsed field.
/// An error means the parser will then attempt to parse this as an undelimited field.
fn parse_delimited<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    quotes: &'a (char, char),
    field_terminator: &'a str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| {
        terminated(
            delimited(
                char(quotes.0),
                map(
                    opt(escaped(
                        take_while1(|c: char| c != '\\' && c != quotes.1),
                        '\\',
                        satisfy(|c| c == '\\' || c == quotes.1),
                    )),
                    |inner| inner.unwrap_or(""),
                ),
                char(quotes.1),
            ),
            peek(alt((
                parse_field_delimiter(field_terminator),
                preceded(space0, eof),
            ))),
        )(input)
    }
}

/// An undelimited value is all the text until our field_delimiter, or if it is the last value in the line,
/// just take the rest of the string.
fn parse_undelimited<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    field_delimiter: &'a str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| map(alt((take_until(field_delimiter), rest)), |s: &str| s.trim())(input)
}

fn quoted<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    quotes: &'a [(char, char)],
    delimiter: &'a str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| {
        let mut last_err = None;
        for quotes in quotes {
            match parse_delimited(quotes, delimiter)(input) {
                done @ Ok(..) => return done,
                err @ Err(..) => last_err = Some(err), // continue
            }
        }
        last_err.unwrap()
    }
}

fn match_re_or_empty<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    value_re: &'a Regex,
    field_delimiter: &'a str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| {
        re_find(value_re.clone())(input).or_else(|_e: nom::Err<E>| {
            parse_undelimited(field_delimiter)(input).map(|(rest, _v)| (rest, ""))
        })
    }
}

/// Parses the value.
/// The value has two parsing strategies.
///
/// 1. Parse as a delimited field - currently the delimiter is hardcoded to a `"`.
/// 2. If it does not start with one of the trim values, it is not a delimited field and we parse up to
///    the next field_delimiter or the eof.
///
fn parse_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    field_delimiter: &'a str,
    quotes: &'a [(char, char)],
    non_quoted_re: Option<&'a Regex>,
) -> impl Fn(&'a str) -> IResult<&'a str, Value, E> {
    move |input| match non_quoted_re {
        Some(re) => map(
            alt((
                quoted(quotes, field_delimiter),
                match_re_or_empty(re, field_delimiter),
            )),
            Into::into,
        )(input),
        None => map(
            alt((
                quoted(quotes, field_delimiter),
                parse_undelimited(field_delimiter),
            )),
            Into::into,
        )(input),
    }
}

/// Parses the key.
/// Parsing strategies are the same as parse_value, but we don't need to convert the result to a `Value`.
fn parse_key<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    key_value_delimiter: &'a str,
    quotes: &'a [(char, char)],
    non_quoted_re: Option<&'a Regex>,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, E> {
    move |input| match non_quoted_re {
        Some(re) => alt((quoted(quotes, key_value_delimiter), re_find(re.to_owned())))(input),
        None => alt((
            quoted(quotes, key_value_delimiter),
            parse_undelimited(key_value_delimiter),
        ))(input),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use regex::Regex;

    #[test]
    fn test_parse() {
        assert_eq!(
            Ok(vec![
                ("ook".to_string(), "pook".into()),
                (
                    "@timestamp".to_string(),
                    "2020-12-31T12:43:22.2322232Z".into()
                ),
                ("key#hash".to_string(), "value".into()),
                ("key=with=special=characters".to_string(), "value".into()),
                ("key".to_string(), "with special=characters".into()),
            ]),
            parse(
                r#"ook=pook @timestamp=2020-12-31T12:43:22.2322232Z key#hash=value "key=with=special=characters"=value key="with special=characters""#,
                "=",
                &[" "],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                false,
            )
        );
    }

    #[test]
    fn test_parse_key_value() {
        assert_eq!(
            Ok(("", ("ook".to_string(), "pook".into()))),
            parse_key_value::<VerboseError<&str>>(
                "=",
                " ",
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                false
            )("ook=pook")
        );

        assert_eq!(
            Ok(("", ("key".to_string(), "".into()))),
            parse_key_value::<VerboseError<&str>>(
                "=",
                " ",
                &[('"', '"')],
                None,
                Whitespace::Strict,
                false
            )("key=")
        );
    }

    #[test]
    fn test_parse_key_values() {
        assert_eq!(
            Ok(vec![
                ("ook".to_string(), "pook".into()),
                ("onk".to_string(), "ponk".into())
            ]),
            parse(
                "ook=pook onk=ponk",
                "=",
                &[" "],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                false
            )
        );
    }

    #[test]
    fn test_parse_key_values_strict() {
        assert_eq!(
            Ok(vec![
                ("ook".to_string(), "".into()),
                ("onk".to_string(), "ponk".into())
            ]),
            parse(
                "ook= onk=ponk",
                "=",
                &[" "],
                &[('"', '"')],
                None,
                Whitespace::Strict,
                false
            )
        );
    }

    #[test]
    fn test_parse_standalone_key() {
        assert_eq!(
            Ok(vec![
                ("foo".to_string(), "bar".into()),
                ("foobar".to_string(), Value::Boolean(true))
            ]),
            parse(
                "foo:bar ,   foobar   ",
                ":",
                &[","],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                true
            )
        );
    }

    #[test]
    fn test_multiple_standalone_key() {
        assert_eq!(
            Ok(vec![
                ("foo".to_string(), "bar".into()),
                ("foobar".to_string(), Value::Boolean(true)),
                ("bar".to_string(), "baz".into()),
                ("barfoo".to_string(), Value::Boolean(true)),
            ]),
            parse(
                "foo=bar foobar bar=baz barfoo",
                "=",
                &[" "],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                true
            )
        );
    }

    #[test]
    fn test_only_standalone_key() {
        assert_eq!(
            Ok(vec![
                ("foo".to_string(), Value::Boolean(true)),
                ("bar".to_string(), Value::Boolean(true)),
                ("foobar".to_string(), Value::Boolean(true)),
                ("baz".to_string(), Value::Boolean(true)),
                ("barfoo".to_string(), Value::Boolean(true)),
            ]),
            parse(
                "foo bar foobar baz barfoo",
                "=",
                &[" "],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                true
            )
        );
    }

    #[test]
    fn test_parse_single_standalone_key() {
        assert_eq!(
            Ok(vec![("foobar".to_string(), Value::Boolean(true))]),
            parse(
                "foobar",
                ":",
                &[","],
                &[('"', '"')],
                None,
                Whitespace::Lenient,
                true
            )
        );
    }

    #[test]
    fn test_parse_standalone_key_strict() {
        assert_eq!(
            Ok(vec![
                ("foo".to_string(), "bar".into()),
                ("foobar".to_string(), Value::Boolean(true))
            ]),
            parse(
                "foo:bar ,   foobar   ",
                ":",
                &[","],
                &[('"', '"')],
                None,
                Whitespace::Strict,
                true
            )
        );
    }

    #[test]
    fn test_parse_key() {
        // delimited
        assert_eq!(
            Ok(("", "noog")),
            parse_key::<VerboseError<&str>>("=", &[('"', '"')], None)(r#""noog""#)
        );

        // undelimited
        assert_eq!(
            Ok(("", "noog")),
            parse_key::<VerboseError<&str>>("=", &[('"', '"')], None)("noog")
        );
    }

    #[test]
    fn test_parse_value() {
        // delimited
        assert_eq!(
            Ok(("", "noog".into())),
            parse_value::<VerboseError<&str>>(" ", &[('"', '"')], None)(r#""noog""#)
        );

        // undelimited
        assert_eq!(
            Ok(("", "noog".into())),
            parse_value::<VerboseError<&str>>(" ", &[('"', '"')], None)("noog")
        );

        // empty delimited
        assert_eq!(
            Ok(("", "".into())),
            parse_value::<VerboseError<&str>>(" ", &[('"', '"')], None)(r#""""#)
        );

        // empty undelimited
        assert_eq!(
            Ok(("", "".into())),
            parse_value::<VerboseError<&str>>(" ", &[('"', '"')], None)("")
        );
    }

    #[test]
    fn test_parse_delimited_with_internal_quotes() {
        assert!(parse_delimited::<VerboseError<&str>>(&('"', '"'), "=")(r#""noog" nonk"#).is_err());
    }

    #[test]
    fn test_parse_delimited_with_internal_delimiters() {
        assert_eq!(
            Ok(("", "noog nonk")),
            parse_delimited::<VerboseError<&str>>(&('"', '"'), " ")(r#""noog nonk""#)
        );
    }

    #[test]
    fn test_parse_undelimited_with_quotes() {
        assert_eq!(
            Ok(("", r#""noog" nonk"#)),
            parse_undelimited::<VerboseError<&str>>(":")(r#""noog" nonk"#)
        );
    }

    // DD examples from https://docs.datadoghq.com/logs/log_configuration/parsing/?tab=filters#key-value-or-logfmt
    #[test]
    fn test_parse_dd_examples() {
        let default_value_re = Regex::new(r"^[\w.\-_@]+").unwrap();
        let default_quotes = &[('"', '"'), ('\'', '\''), ('<', '>')];

        let default_key_value_delimiter = "=";
        let default_field_delimiters = &[" ", ",", ";"];

        assert_eq!(
            Ok(vec![("key".to_string(), "valueStr".into()),]),
            parse(
                "key=valueStr",
                default_key_value_delimiter,
                default_field_delimiters,
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("key".to_string(), "valueStr".into()),]),
            parse(
                "key=<valueStr>",
                default_key_value_delimiter,
                default_field_delimiters,
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("key".to_string(), "valueStr".into()),]),
            parse(
                r#""key"="valueStr""#,
                default_key_value_delimiter,
                default_field_delimiters,
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("key".to_string(), "valueStr".into()),]),
            parse(
                "key:valueStr",
                ":",
                default_field_delimiters,
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("key".to_string(), "/valueStr".into()),]),
            parse(
                r#"key:"/valueStr""#,
                ":",
                default_field_delimiters,
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("/key".to_string(), "/valueStr".into()),]),
            parse(
                r#"/key:/valueStr"#,
                ":",
                default_field_delimiters,
                default_quotes,
                Some(&Regex::new(r"^[\w.\-_@/]+").unwrap()),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![("key".to_string(), "valueStr".into()),]),
            parse(
                r#"key:={valueStr}"#,
                ":=",
                default_field_delimiters,
                &[('{', '}')],
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![
                ("key1".to_string(), "value1".into()),
                ("key2".to_string(), "value2".into())
            ]),
            parse(
                r#"key1=value1|key2=value2"#,
                default_key_value_delimiter,
                &["|"],
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
        assert_eq!(
            Ok(vec![
                ("key1".to_string(), "value1".into()),
                ("key2".to_string(), "value2".into())
            ]),
            parse(
                r#"key1="value1"|key2="value2""#,
                default_key_value_delimiter,
                &["|"],
                default_quotes,
                Some(&default_value_re),
                Whitespace::Strict,
                false
            )
        );
    }
}
