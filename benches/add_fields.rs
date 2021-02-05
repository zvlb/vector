//! This mod provides useful utilities for writing benchmarks.

use criterion::{criterion_group, criterion_main, Criterion};

use vector::Event;

mod test_framework;

use test_framework::transform::BenchmarkGroupExt;

criterion_group!(benches, benchmark);
criterion_main!(benches);

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("by_field_amount");

    let event = {
        let mut e = Event::new_empty_log();
        e.as_mut_log().insert("message", "test message".to_string());
        e
    };
    let events: Vec<_> = std::iter::repeat(event).clone().take(3).collect();

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

// mod testevent {
//     use std::iter::FromIterator;

//     pub fn gen<T: FromIterator<Item = Event>>(amount: usize) -> T {}
// }
