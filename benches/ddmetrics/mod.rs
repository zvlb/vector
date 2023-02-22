use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vector::{
    event::{Metric, MetricKind, MetricValue},
    sinks::datadog::metrics::collapse_counters_by_series_and_timestamp,
};

fn create_counter(name: &str, value: f64) -> Metric {
    Metric::new(
        name,
        MetricKind::Incremental,
        MetricValue::Counter { value },
    )
}

fn benchmark_empty(c: &mut Criterion) {
    c.bench_function("empty", |b| {
        b.iter(|| {
            let input = Vec::new();
            collapse_counters_by_series_and_timestamp(black_box(input))
        })
    });
}

fn benchmark_single_counter(c: &mut Criterion) {
    c.bench_function("single_counter", |b| {
        b.iter(|| {
            let input = vec![create_counter("basic", 42.0)];
            collapse_counters_by_series_and_timestamp(black_box(input))
        })
    });
}

fn benchmark_collapse_counter(c: &mut Criterion) {
    c.bench_function("collapse_counter", |b| {
        b.iter(|| {
            let counter_value = 42.0;
            let input = vec![
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
                create_counter("basic", counter_value),
            ];
            collapse_counters_by_series_and_timestamp(black_box(input))
        })
    });
}

criterion_group!(
    name = benches;
    // encapsulates inherent CI noise we saw in
    // https://github.com/vectordotdev/vector/issues/5394
    //config = Criterion::default().noise_threshold(0.05);
    config = Criterion::default();
    targets = benchmark_empty, benchmark_single_counter, benchmark_collapse_counter
);

criterion_main! {
    benches,
}
