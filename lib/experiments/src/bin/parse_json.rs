use bytes::Bytes;
use shared::TimeZone;
use std::collections::BTreeMap;
use vector_core::event::{self, Event, EventMetadata, LogEvent, VrlTarget};
use vrl::{Program, Runtime};

const JSON: &[u8] = r#"{"key": "value"}"#.as_bytes();
const PROGRAM: &str = r#". = parse_json!(.message)"#;

fn main() {
    let program: Program = vrl::compile(PROGRAM, &vrl_stdlib::all()).unwrap();

    // this loop simulates the call of `<Remap as FunctionTransform>::transform`

    let timezone = TimeZone::default();
    let mut map: BTreeMap<String, event::Value> = BTreeMap::new();
    map.insert(
        "message".to_owned(),
        event::Value::Bytes(Bytes::from_static(JSON)),
    );
    let metadata = EventMetadata::default();

    let log_event = LogEvent::from_parts(map, metadata);
    let event = Event::Log(log_event);

    loop {
        let mut runtime = Runtime::default();
        let mut target: VrlTarget = event.clone().into();
        let _ = runtime.resolve(&mut target, &program, &timezone).unwrap();
    }
}
