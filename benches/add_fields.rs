//! This mod provides useful utilities for writing benchmarks.

use criterion::{criterion_group, criterion_main, AxisScale, Criterion, PlotConfiguration};

use vector::{event, Event};

mod test_framework;

use self::test_framework::transform::bench::BenchmarkGroupExt;

criterion_group!(benches, benchmark, benchmark_qwe);
criterion_main!(benches);

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("by_field_amount");

    let event = {
        let mut e = Event::new_empty_log();
        e.as_mut_log().insert("message", "test message".to_string());
        e
    };
    let events: Vec<_> = std::iter::repeat(event).take(3).collect();

    let cases = vec![1, 2, 10, 100, 1000];

    for case in cases {
        {
            let fields: String = (0..case)
                .map(|n| format!("fields.a{} = \"{}\"\n", n, n))
                .collect();

            let config = format!(
                r#"
                    type = "add_fields"
                    {}
                    overwrite = false
                "#,
                fields
            );

            group.sampling_mode(criterion::SamplingMode::Linear);
            group.bench_function_transform(
                "by_fields",
                Some(case.to_string()),
                &config,
                events.clone(),
                events.clone(),
            );
        }
        {
            let fields: String = (0..case)
                .map(|n| format!("fields.a{} = \"{{ message }}\"\n", n))
                .collect();

            let config = format!(
                r#"
                    type = "add_fields"
                    {}
                    overwrite = false
                "#,
                fields
            );

            group.sampling_mode(criterion::SamplingMode::Linear);
            group.bench_function_transform(
                "by_template_fields",
                Some(case.to_string()),
                &config,
                events.clone(),
                events.clone(),
            );
        }
    }

    group.finish();
}

fn benchmark_qwe(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");

    fn mkevent(s: &str, is_partial: bool) -> Event {
        let mut event = Event::new_empty_log();
        event.as_mut_log().insert("message", s.to_string());
        if is_partial {
            event
                .as_mut_log()
                .insert(event::PARTIAL, "true".to_string());
        }
        event
    }

    let events = vec![mkevent("a", true), mkevent("b", false)]
        .into_iter()
        .cycle();

    let cases = vec![2, 20, 200, 1000, 2000, 4000, 20000, 200000];

    for case in cases {
        let events = events.clone().take(case).collect();
        group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
        group.bench_task_transform(
            "by_input_size",
            Some(case.to_string()),
            r#"
                type = "merge"
            "#,
            events,
        );
    }

    group.finish();
}
