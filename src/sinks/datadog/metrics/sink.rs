use std::{fmt, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{
    future::ready,
    stream::{self, BoxStream},
    StreamExt,
};
use tower::Service;
use vector_common::finalization::EventFinalizers;
use vector_core::{
    event::{Event, Metric, MetricValue},
    partition::Partitioner,
    sink::StreamSink,
    stream::{BatcherSettings, DriverResponse},
};

use super::{
    config::DatadogMetricsEndpoint, normalizer::DatadogMetricsNormalizer,
    request_builder::DatadogMetricsRequestBuilder, service::DatadogMetricsRequest,
};
use crate::{
    internal_events::DatadogMetricsEncodingError,
    sinks::util::{
        buffer::metrics::sort::sort_for_compression,
        buffer::metrics::{AggregatedSummarySplitter, MetricSplitter},
        SinkBuilderExt,
    },
};

/// Partitions metrics based on which Datadog API endpoint that they are sent to.
///
/// Generally speaking, all "basic" metrics -- counter, gauge, set, aggregated summary-- are sent to
/// the Series API, while distributions, aggregated histograms, and sketches (hehe) are sent to the
/// Sketches API.
struct DatadogMetricsTypePartitioner;

impl Partitioner for DatadogMetricsTypePartitioner {
    type Item = Metric;
    type Key = (Option<Arc<str>>, DatadogMetricsEndpoint);

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let endpoint = match item.data().value() {
            MetricValue::Counter { .. } => DatadogMetricsEndpoint::Series,
            MetricValue::Gauge { .. } => DatadogMetricsEndpoint::Series,
            MetricValue::Set { .. } => DatadogMetricsEndpoint::Series,
            MetricValue::Distribution { .. } => DatadogMetricsEndpoint::Sketches,
            MetricValue::AggregatedHistogram { .. } => DatadogMetricsEndpoint::Sketches,
            MetricValue::AggregatedSummary { .. } => DatadogMetricsEndpoint::Series,
            MetricValue::Sketch { .. } => DatadogMetricsEndpoint::Sketches,
        };
        (item.metadata().datadog_api_key(), endpoint)
    }
}

pub(crate) struct DatadogMetricsSink<S> {
    service: S,
    request_builder: DatadogMetricsRequestBuilder,
    batch_settings: BatcherSettings,
    protocol: String,
}

impl<S> DatadogMetricsSink<S>
where
    S: Service<DatadogMetricsRequest> + Send,
    S::Error: fmt::Debug + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse,
{
    /// Creates a new `DatadogMetricsSink`.
    pub const fn new(
        service: S,
        request_builder: DatadogMetricsRequestBuilder,
        batch_settings: BatcherSettings,
        protocol: String,
    ) -> Self {
        DatadogMetricsSink {
            service,
            request_builder,
            batch_settings,
            protocol,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut splitter: MetricSplitter<AggregatedSummarySplitter> = MetricSplitter::default();

        input
            // Convert `Event` to `Metric` so we don't have to deal with constant conversions.
            .filter_map(|event| ready(event.try_into_metric()))
            // Split aggregated summaries into individual metrics for count, sum, and the quantiles, which lets us
            // ensure that aggregated summaries effectively make it through normalization, as we can't actually
            // normalize them and so they would be dropped during normalization otherwise.
            .flat_map(|metric| stream::iter(splitter.split(metric)))
            // Converts "absolute" metrics to "incremental", and converts distributions and aggregated histograms into
            // sketches so that we can send them in a more DD-native format and thus avoid needing to directly specify
            // what quantiles to generate, etc.
            .normalized_with_default::<DatadogMetricsNormalizer>()
            // We batch metrics by their endpoint: series endpoint for counters, gauge, and sets vs sketch endpoint for
            // distributions, aggregated histograms, and sketches.
            .batched_partitioned(DatadogMetricsTypePartitioner, self.batch_settings)
            // Aggregate counters with identical timestamps, otherwise identical counters (same
            // series and same timestamp, when rounded to whole seconds) will be dropped in a
            // last-write-wins situation when they hit the DD metrics intake.
            .map(|((api_key, endpoint), mut metrics)| {
                collapse_counters_by_series_and_timestamp(&mut metrics);
                ((api_key, endpoint), metrics)
            })
            // Sort metrics by name, which significantly improves HTTP compression.
            .map(|((api_key, endpoint), mut metrics)| {
                sort_for_compression(&mut metrics);
                ((api_key, endpoint), metrics)
            })
            // We build our requests "incrementally", which means that for a single batch of metrics, we might generate
            // N requests to send them all, as Datadog has API-level limits on payload size, so we keep adding metrics
            // to a request until we reach the limit, and then create a new request, and so on and so forth, until all
            // metrics have been turned into a request.
            .incremental_request_builder(self.request_builder)
            // This unrolls the vector of request results that our request builder generates.
            .flat_map(stream::iter)
            // Generating requests _can_ fail, so we log and filter out errors here.
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        let (error_message, error_code, dropped_events) = e.into_parts();
                        emit!(DatadogMetricsEncodingError {
                            error_message,
                            error_code,
                            dropped_events: dropped_events as usize,
                        });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            // Finally, we generate the driver which will take our requests, send them off, and appropriately handle
            // finalization of the events, and logging/metrics, as the requests are responded to.
            .into_driver(self.service)
            .protocol(self.protocol)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for DatadogMetricsSink<S>
where
    S: Service<DatadogMetricsRequest> + Send,
    S::Error: fmt::Debug + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        // Rust has issues with lifetimes and generics, which `async_trait` exacerbates, so we write
        // a normal async fn in `DatadogMetricsSink` itself, and then call out to it from this trait
        // implementation, which makes the compiler happy.
        self.run_inner(input).await
    }
}

/// Collapse duplicate counters (those with the same MetricSeries (name, namespace, tags)), within
/// the input array. Non-counters are un-modified. The function is designed for efficiency by not
/// requiring a sorted input array, as a consequence the order of metrics in the input array is
/// modified in the event of any collapsed metrics.
///
// NOTE: Astute observers may recognize that this behavior could also be achieved by using
// `Vec::dedup_by`, but the clincher is that `dedup_by` requires a sorted vector to begin with.
//
// This function is designed to collapse duplicate counters even if the metrics are unsorted,
// which leads to a measurable boost in performance, being nearly 35% faster than `dedup_by`
// when the inputs are sorted, and up to 50% faster when the inputs are unsorted.
//
// These numbers are based on sorting a newtype wrapper around the metric instead of the metric
// itself, which does involve allocating a string in our tests. _However_, sorting the `Metric`
// directly is not possible without a customized `PartialOrd` implementation, as some of the
// nested fields containing `f64` values makes it underivable, and I'm not 100% sure that we
// could/would want to have a narrowly-focused impl of `PartialOrd` on `Metric` to fit this use
// case (metric type -> metric name -> metric timestamp, nothing else) vs being able to sort
// metrics by name first, etc. Then there's the potential issue of the reordering of fields
// changing the ordering behavior of `Metric`... and it just felt easier to write this tailored
// algorithm for the use case at hand.
fn collapse_counters_by_series_and_timestamp(metrics: &mut Vec<Metric>) {
    let og_len = metrics.len();

    // nothing to collapse if less than two elements
    if og_len < 2 {
        return;
    }

    // main "outer loop" indexing iterator
    let mut idx = 0;

    // used for metrics without existing timestamps
    let now_ts = Utc::now().timestamp();

    let mut total_collapsed = 0;

    // the actual tail index that we stop iterating at will change as we collapse metrics
    let mut tail = og_len;

    // For each metric, see if it's a counter. If so, we check the rest of the metrics
    // _after_ it to see if they share the same series _and_ timestamp, when converted
    // to a Unix timestamp. If they match, we take that counter's value and merge it
    // with our "current" counter metric, and then drop the secondary one from the
    // vector.
    //
    // For any non-counter, we simply ignore it and leave it as-is.
    while idx < tail - 1 {
        if let MetricValue::Counter { .. } = metrics[idx].value() {
            // Maintain a separate tracking of the current index, for lookup in the array post
            // collapsing, since the `idx` is modified within the aggregation function.
            let curr_idx = idx;

            // Split the metrics array into two mutable slices. This allows us to take an
            // immutable reference to the "current" metric that will be used to compare against
            // the right hand slice, which needs to be mutable as the helper function takes
            // measures to reduce the number of loop iterations by simulating `swap_remove()`.
            let (lhs, rhs) = metrics.split_at_mut(idx + 1);

            let (n_collapsed, accumulated_value, accumulated_finalizers) =
                collapse_counters_matching_current(
                    &lhs[idx],
                    rhs,
                    &mut idx,
                    total_collapsed,
                    now_ts,
                );

            // If we collapsed any during the aggregation phase, update the original counter.
            if n_collapsed > 0 {
                total_collapsed += n_collapsed;

                // We do not want to iterate this outer loop over the metrics that we've dropped off
                // the end, as they will have already been processed.
                tail -= n_collapsed;

                let metric = metrics.get_mut(curr_idx).expect("current index must exist");
                match metric.value_mut() {
                    MetricValue::Counter { value } => {
                        *value += accumulated_value;
                        metric
                            .metadata_mut()
                            .merge_finalizers(accumulated_finalizers);
                    }
                    _ => unreachable!("current index must represent a counter"),
                }
            }
        }

        idx += 1;
    }

    // Truncate the original metrics array by the amount that we collapsed.
    // This is more efficient than truncating each time we iterate.
    if total_collapsed > 0 {
        metrics.truncate(og_len - total_collapsed);
    }
}

/// Iterate over each metric _after_ the current one to see if it matches the
/// current counter. A match must be a counter with the same series and timestamp.
/// If it is, we accumulate its value and finalizers and then swap in the element from the current
/// tail position of the array.
///
/// Otherwise, skip it.
fn collapse_counters_matching_current(
    curr_counter: &Metric,
    rhs: &mut [Metric],
    idx: &mut usize,
    dead_end: usize,
    now_ts: i64,
) -> (usize, f64, EventFinalizers) {
    // The accumulated metric data to return from this function
    let mut n_collapsed = 0;
    let mut accumulated_value = 0.0;
    let mut accumulated_finalizers = EventFinalizers::default();

    // References to the current counter for comparison in the inner loop
    let counter_ts = get_timestamp(curr_counter, now_ts);
    let curr_series = curr_counter.series();

    let mut is_disjoint = false;
    let mut rhs_idx = 0;
    let mut tail = rhs.len() - dead_end;

    while rhs_idx < tail {
        let inner_metric = &mut rhs[rhs_idx];
        let mut should_advance = true;

        if let MetricValue::Counter { value } = inner_metric.value() {
            let other_counter_ts = get_timestamp(inner_metric, now_ts);

            // Order of comparison matters here. Compare to the timespamps first as the series
            // comparison is much more expensive.
            if counter_ts == other_counter_ts && curr_series == inner_metric.series() {
                // Accumulating the counter's  value, and it's finalizers.
                accumulated_value += value;
                accumulated_finalizers.merge(inner_metric.metadata_mut().take_finalizers());

                // Collapse the counter by cloning in the metric from the moving tail of the array to replace
                // the collapsed metric.
                rhs[rhs_idx] = rhs[tail - 1].clone();
                tail -= 1;
                should_advance = false;
                n_collapsed += 1;
            } else {
                // We hit a counter that _doesn't_ match, but we can't just skip
                // it because we also need to evaluate it against all the
                // counters that come after it, so we only increment the index
                // for this inner loop.
                //
                // As well, we mark ourselves to stop incrementing the outer
                // index if we find more counters to accumulate, because we've
                // hit a disjoint counter here. While we may be continuing to
                // shrink the count of remaining metrics from accumulating,
                // we have to ensure this counter we just visited is visited by
                // the outer loop.
                is_disjoint = true;
            }
        }

        if should_advance {
            rhs_idx += 1;

            if !is_disjoint {
                *idx += 1;
            }
        }
    }

    (n_collapsed, accumulated_value, accumulated_finalizers)
}

/// Returns the Unix epoch representation of the counter or the provided default
fn get_timestamp(counter: &Metric, default: i64) -> i64 {
    counter
        .data()
        .timestamp()
        .map(|dt| dt.timestamp())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {

    use chrono::{DateTime, Duration, Utc};
    use proptest::prelude::*;
    use vector_core::event::{Metric, MetricKind, MetricValue};

    use super::collapse_counters_by_series_and_timestamp;

    fn arb_collapsible_metrics() -> impl Strategy<Value = Vec<Metric>> {
        let ts = Utc::now();

        any::<Vec<(u16, MetricValue)>>().prop_map(move |values| {
            values
                .into_iter()
                .map(|(id, value)| {
                    let name = format!("{}-{}", value.as_name(), id);
                    Metric::new(name, MetricKind::Incremental, value).with_timestamp(Some(ts))
                })
                .collect()
        })
    }

    fn create_counter(name: &str, value: f64) -> Metric {
        Metric::new(
            name,
            MetricKind::Incremental,
            MetricValue::Counter { value },
        )
    }

    fn create_gauge(name: &str, value: f64) -> Metric {
        Metric::new(name, MetricKind::Incremental, MetricValue::Gauge { value })
    }

    #[test]
    fn collapse_no_metrics() {
        let mut actual = Vec::new();
        let expected = actual.clone();
        collapse_counters_by_series_and_timestamp(&mut actual);

        assert_eq!(expected, actual);
    }

    #[test]
    fn collapse_single_metric() {
        let mut actual = vec![create_counter("basic", 42.0)];
        let expected = actual.clone();
        collapse_counters_by_series_and_timestamp(&mut actual);

        assert_eq!(expected, actual);
    }

    /// Tests collapse_counters_by_series_and_timestamp() where every
    /// gauge is collapsible. Expect no collapsing since not counters.
    #[test]
    fn collapse_identical_metrics_gauge() {
        {
            let mut actual = vec![create_gauge("basic", 42.0), create_gauge("basic", 42.0)];
            let expected = actual.clone();
            collapse_counters_by_series_and_timestamp(&mut actual);

            assert_eq!(expected, actual);
        }

        {
            let gauge_value = 41.0;
            let mut actual = vec![
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
                create_gauge("basic", gauge_value),
            ];
            let expected = actual.clone();
            collapse_counters_by_series_and_timestamp(&mut actual);

            assert_eq!(expected, actual);
        }
    }

    /// Tests collapse_counters_by_series_and_timestamp() where every
    /// counter is collapsible and do not have timestamps.
    #[test]
    fn collapse_identical_metrics_counter() {
        let counter_value = 42.0;
        let mut actual = vec![
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
            create_counter("basic", counter_value),
        ];

        let expected_counter_value = actual.len() as f64 * counter_value;
        let expected = vec![create_counter("basic", expected_counter_value)];
        collapse_counters_by_series_and_timestamp(&mut actual);

        assert_eq!(expected, actual);
    }

    /// Tests collapse_counters_by_series_and_timestamp() where every other counter
    /// is collapsible and the remaining have unique, existing timestamps.
    #[test]
    fn collapse_identical_metrics_counter_even() {
        let counter_value = 1.0;

        let mut actual = vec![];
        let mut expected = vec![];

        let now = Utc::now();

        let n = 10;

        expected.push(create_counter("basic", counter_value * (n as f64 / 2.0)));

        for i in 0..n {
            if i % 2 == 0 {
                actual.push(create_counter("basic", counter_value));
            } else {
                actual.push(
                    create_counter("basic", counter_value)
                        .with_timestamp(Some(now + Duration::seconds(i))),
                );
                expected.push(
                    create_counter("basic", counter_value)
                        .with_timestamp(Some(now + Duration::seconds(i))),
                );
            }
        }

        collapse_counters_by_series_and_timestamp(&mut actual);

        let expected_value = if let MetricValue::Counter { value } = expected[0].value() {
            *value
        } else {
            1.0
        };

        let actual_value = if let MetricValue::Counter { value } = actual[0].value() {
            *value
        } else {
            0.0
        };

        assert_eq!(expected_value, actual_value);
        assert_eq!(expected.len(), actual.len());
    }

    /// Tests collapse_counters_by_series_and_timestamp() where no
    /// counter is collapsible- all have unique, existing timestamps.
    #[test]
    fn collapse_identical_metrics_counter_all_unique() {
        let counter_value = 1.0;
        let mut actual = vec![];
        let now = Utc::now();

        for i in 0..10 {
            actual.push(
                create_counter("basic", counter_value)
                    .with_timestamp(Some(now + Duration::seconds(i))),
            );
        }

        let expected = actual.clone();

        collapse_counters_by_series_and_timestamp(&mut actual);

        assert_eq!(expected, actual);
    }

    #[derive(Eq, Ord, PartialEq, PartialOrd)]
    struct MetricCollapseSort {
        metric_type: &'static str,
        metric_name: String,
        metric_ts: Option<DateTime<Utc>>,
    }

    impl MetricCollapseSort {
        fn from_metric(metric: &Metric) -> Self {
            Self {
                metric_type: metric.value().as_name(),
                metric_name: metric.name().to_string(),
                metric_ts: metric.timestamp(),
            }
        }
    }

    fn collapse_dedup_fn(left: &mut Metric, right: &mut Metric) -> bool {
        let series_eq = left.series() == right.series();
        let timestamp_eq = left.timestamp() == right.timestamp();
        if !series_eq || !timestamp_eq {
            return false;
        }

        match (left.value_mut(), right.value_mut()) {
            (
                MetricValue::Counter { value: left_value },
                MetricValue::Counter { value: right_value },
            ) => {
                // NOTE: The docs for `dedup_by` specify that if `left`/`right` are equal, then
                // `left` is the element that gets removed.
                *right_value += *left_value;
                true
            }
            // Only counters can be equivalent for the purpose of this test.
            _ => false,
        }
    }

    proptest! {
        #[test]
        fn test_counter_collapse(mut actual in arb_collapsible_metrics()) {
            let mut expected_output = actual.clone();
            expected_output.sort_by_cached_key(MetricCollapseSort::from_metric);
            expected_output.dedup_by(collapse_dedup_fn);

            collapse_counters_by_series_and_timestamp(&mut actual);
            actual.sort_by_cached_key(MetricCollapseSort::from_metric);

            prop_assert_eq!(expected_output, actual);
        }
    }
}
