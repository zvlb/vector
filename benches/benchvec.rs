use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BatchSize, BenchmarkId, Criterion,
};
use criterion_linux_perf::{PerfMeasurement, PerfMode};
use smallvec::SmallVec;
use std::mem::{drop, size_of};
use std::time::Duration;

#[derive(Default)]
struct LogEvent([u64; 9]);
#[derive(Default)]
struct Metric([u64; 22]);
enum Event {
    Log(LogEvent),
    #[allow(dead_code)]
    Metric(Metric),
}
enum EventsVec {
    Logs(Vec<LogEvent>),
}
enum EventsSmallVec {
    Logs(SmallVec<[LogEvent; 8]>),
}

impl EventsVec {
    fn into_iter(self) -> impl Iterator<Item = Event> {
        match self {
            Self::Logs(v) => v.into_iter().map(Event::Log),
        }
    }
}

impl EventsSmallVec {
    fn into_iter(self) -> impl Iterator<Item = Event> {
        match self {
            Self::Logs(v) => v.into_iter().map(Event::Log),
        }
    }
}

fn make_vec(events: usize) -> EventsVec {
    let mut result = Vec::with_capacity(events);
    for _ in 0..events {
        result.push(LogEvent([events as u64; 9]));
    }
    EventsVec::Logs(result)
}

fn make_smallvec(events: usize) -> EventsSmallVec {
    let mut result = SmallVec::with_capacity(events);
    for _ in 0..events {
        result.push(LogEvent([events as u64; 9]));
    }
    EventsSmallVec::Logs(result)
}

fn count_vec(events: EventsVec) -> u64 {
    events
        .into_iter()
        .map(|event| match event {
            Event::Log(log) => log.0[1],
            Event::Metric(metric) => metric.0[1],
        })
        .sum()
}

fn count_smallvec(events: EventsSmallVec) -> u64 {
    events
        .into_iter()
        .map(|event| match event {
            Event::Log(log) => log.0[1],
            Event::Metric(metric) => metric.0[1],
        })
        .sum()
}

fn timeit<T: 'static + Measurement>(crit: &mut Criterion<T>) {
    dbg!(size_of::<Event>());
    dbg!(size_of::<EventsVec>());
    dbg!(size_of::<EventsSmallVec>());

    let mut group = crit.benchmark_group("Creation");
    for size in [1, 2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("Vec", size), &size, |bencher, &size| {
            bencher.iter(|| drop(make_vec(size)))
        });
        group.bench_with_input(BenchmarkId::new("SV", size), &size, |bencher, &size| {
            bencher.iter(|| drop(make_smallvec(size)))
        });
    }
    group.finish();

    let mut group = crit.benchmark_group("Iterator");
    for size in [1, 2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("Vec", size), &size, |bencher, &size| {
            bencher.iter_batched(|| make_vec(size), count_vec, BatchSize::SmallInput)
        });
        group.bench_with_input(BenchmarkId::new("SV", size), &size, |bencher, &size| {
            bencher.iter_batched(
                || make_smallvec(size),
                count_smallvec,
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    name = benchvec;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(180))
        // degree of noise to ignore in measurements, here 1%
        .noise_threshold(0.01)
        // likelihood of noise registering as difference, here 5%
        .significance_level(0.05)
        // likelihood of capturing the true runtime, here 95%
        .confidence_level(0.99)
        // total number of bootstrap resamples, higher is less noisy but slower
        .nresamples(100_000)
        // total samples to collect within the set measurement time
        .sample_size(500)
        .with_measurement(PerfMeasurement::new(PerfMode::Cycles));
    targets = timeit
);
criterion_main!(benchvec);
