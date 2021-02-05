use core::panic;

use criterion::{
    measurement::Measurement, BatchSize, Bencher, BenchmarkGroup, BenchmarkId, Throughput,
};

use serde::{Deserialize, Serialize};
use vector::{config::TransformConfig, transforms::Transform};
use vector::{transforms::FunctionTransform, Event};

pub trait BenchmarkGroupExt {
    fn bench_function_transform<ID>(
        &mut self,
        id: ID,
        parameter: Option<String>,
        toml_config: &str,
        prewarm_events: Vec<Event>,
        events: Vec<Event>,
    ) -> &mut Self
    where
        ID: Into<String>;
}

impl<'a, M: Measurement> BenchmarkGroupExt for BenchmarkGroup<'a, M> {
    fn bench_function_transform<ID>(
        &mut self,
        id: ID,
        parameter: Option<String>,
        toml_config: &str,
        prewarm_events: Vec<Event>,
        events: Vec<Event>,
    ) -> &mut Self
    where
        ID: Into<String>,
    {
        let transform_config = parse_config(toml_config);

        let id = format!(
            "transform/{}/{}",
            transform_config.transform_type(),
            id.into()
        );
        let id = match parameter {
            Some(parameter) => BenchmarkId::new(id, parameter),
            None => panic!("cant do"),
        };

        let transform = build(transform_config.as_ref());
        let transform_function = transform.into_function();

        self.throughput(Throughput::Elements(events.len() as u64));
        self.bench_function(id, move |b| {
            run_function_transform(
                b,
                transform_function.clone(),
                prewarm_events.as_slice(),
                events.as_slice(),
            );
        });

        self
    }
}

/// `prewarm_events` and `events` could be `impl Iterator<Item = Event>`, but
/// we want to eliminate the extra variance from the runtime, so we fixed those
/// to be `&[Event]`.
pub fn run_function_transform<M: Measurement>(
    b: &mut Bencher<'_, M>,
    component: Box<dyn FunctionTransform>,
    prewarm_events: &[Event],
    events: &[Event],
) {
    b.iter_batched(
        || {
            let mut out: Vec<Event> = Vec::with_capacity(1);
            let mut component = component.clone();
            for event in prewarm_events.iter().cloned() {
                component.transform(&mut out, event);
                out.clear();
            }
            let events: Vec<_> = events.iter().cloned().collect();
            (component, out, events)
        },
        |(mut component, mut out, events)| {
            for event in events {
                component.transform(&mut out, event);
                out.clear();
            }
            out
        },
        BatchSize::SmallInput,
    )
}

#[derive(Deserialize, Serialize, Debug)]
struct Parser {
    #[serde(flatten)]
    pub inner: Box<dyn TransformConfig>,
}

pub fn parse_config(toml_config: &str) -> Box<dyn TransformConfig> {
    let parser: Parser =
        toml::from_str(toml_config).expect("you must pass a valid config in benches");
    parser.inner
}

pub fn build(transform_config: &dyn TransformConfig) -> Transform {
    futures::executor::block_on(transform_config.build()).expect("transform must build")
}
