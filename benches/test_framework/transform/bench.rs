use criterion::{
    black_box, measurement::Measurement, BatchSize, Bencher, BenchmarkGroup, BenchmarkId,
    Throughput,
};

use futures::StreamExt;
use vector::{
    test_util::runtime,
    transforms::{FunctionTransform, TaskTransform},
    Event,
};

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

    fn bench_task_transform<ID>(
        &mut self,
        id: ID,
        parameter: Option<String>,
        toml_config: &str,
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
        let transform_config = super::parse_config(toml_config);

        let id = format!(
            "transform/{}/{}",
            transform_config.transform_type(),
            id.into()
        );
        let id = match parameter {
            Some(parameter) => BenchmarkId::new(id, parameter),
            None => panic!("cant do"),
        };

        let transform = super::build(transform_config.as_ref());
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

    fn bench_task_transform<ID>(
        &mut self,
        id: ID,
        parameter: Option<String>,
        toml_config: &str,
        events: Vec<Event>,
    ) -> &mut Self
    where
        ID: Into<String>,
    {
        let transform_config = super::parse_config(toml_config);

        let id = format!(
            "transform/{}/{}",
            transform_config.transform_type(),
            id.into()
        );
        let id = match parameter {
            Some(parameter) => BenchmarkId::new(id, parameter),
            None => panic!("cant do"),
        };

        let transform_task_builder = || {
            let transform = super::build(transform_config.as_ref());
            transform.into_task()
        };

        self.throughput(Throughput::Elements(events.len() as u64));
        self.bench_function(id, move |b| {
            run_task_transform(b, transform_task_builder, events.as_slice());
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
            let events = events.to_vec();
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

/// `events` could be `impl Iterator<Item = Event>`, but we want to eliminate
/// the extra variance from the runtime, so we fixed those to be `&[Event]`.
pub fn run_task_transform<M: Measurement, F>(
    b: &mut Bencher<'_, M>,
    component_builder: F,
    events: &[Event],
) where
    F: Fn() -> Box<dyn TaskTransform>,
{
    let component_builder = &component_builder;
    b.iter_batched(
        move || {
            let mut rt = runtime();
            let rest = rt.block_on(async move {
                let component = component_builder();
                let (in_tx, in_rx) = tokio::sync::mpsc::channel(1);
                let out_stream = component.transform(Box::pin(in_rx));
                let events = events.to_vec();
                (in_tx, out_stream, events)
            });
            (rt, rest)
        },
        |(mut rt, (mut in_tx, mut out_stream, events))| {
            rt.block_on(async {
                let in_fut = tokio::spawn(async move {
                    for event in events {
                        in_tx.send(event).await.unwrap();
                    }
                    drop(in_tx);
                });
                let out_fut = tokio::spawn(async move {
                    while let Some(event) = out_stream.next().await {
                        let _ = black_box(event);
                    }
                    drop(out_stream);
                });
                let (a, b) = futures::future::join(in_fut, out_fut).await;
                a.unwrap();
                b.unwrap();
            })
        },
        BatchSize::SmallInput,
    )
}
