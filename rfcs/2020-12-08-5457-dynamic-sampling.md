# RFC 5457 - <2020-12-08> - More robust sampling using the `sample` transform

Vector currently provides some limited sampling capabilities via the [`sample`][sample] transform.
But this transform has some fundamental limitations that we should seek to overcome:

* You can only create sampling "buckets" by matching on a [`key_field`][key_field].
* You can only apply **constant sampling** to buckets, i.e. the [`rate`][rate] that you set remains
  fixed at 1/N, with no regard for event traffic patterns.

> Sampling **buckets** are sub-streams of an event stream into which events are routed based on the
> content of those events.

In this RFC I propose two updates to the `sample` transform:

1. The ability to create sampling buckets in a much more fine-grained way. I propose two new
  options:
    1. Using Vector Remap Language conditions to create buckets, e.g.
       `.status == 200 && .level == "warning"`.
    2. Allowing key-based sampling across multiple fields rather than just one, as with `key_field`.
2. The addition of [**dynamic sampling**][dynamic_sampling] capabilities. Dynamic sampling adjusts
  the sample rate over time based on traffic patterns, decreasing the sampling rate by a specified
  factor when events arrive in a bucket more frequently.

## Scope

This RFC proposes improvements to the [`sample`][sample] transform, including new configuration
parameters and the introduction of the **bucket** concept to Vector. The proposed changes are fully
compatible with Vector's core processing pipeline and configuration system and also with VRL in its
current form, though we should remain open to adding sampling-specific functionality should the need
or demand arise in the future.

### Non-goals

The purpose of the RFC is to achieve consensus around a set of desired features. It does *not*
cover:

* Specific implementation details on the Rust side, though it does point to some prior art that may
  give us a head start.
* Finalized names for user-facing terms and configuration parameters, which inevitably undergo
  change during the development process. The terms here should be taken as suggestive.

### Domains

Improved sampling has potential implications for both logging and tracing. It does *not* have
implications for metrics, as metrics are already a form of "sampling" in themselves and it's not
clear what it would mean to sample metrics.

But despite sampling being germane to tracing, this RFC only covers logging. Tracing is distinct
enough that any overlap between the improvements proposed here and the tracing domain would be happy
accidents. Most fundamentally, log events can be treated as isolated atoms whereas traces by
definition consist of events (spans) united by a common identifier. When Vector moves into tracing,
we'll most likely need to take up the sampling question afresh.

### Feature parity

To my knowledge, this RFC doesn't propose any envelope-pushing features that aren't available in
other systems in the observability space. The goal here is more to bring Vector in line with what I
take to be the state of the art than to push Vector into new territory, as we've done in other
domains.

## Conceptual background

Sampling essentially means retaining only a specific subset of an event stream and omitting the
rest. The goal of sampling is to reduce costs by cutting out noise while retaining enough signal to
satisfy the use case at hand. Sample too aggressively and you compromise insight; sample too
sheepishly and you throw resources away. But if undertaken intelligently and with the right tools,
sampling lets you have your cake 🍰 (process fewer events) and eat it too 🍴 (get the insight you
need).

Some common rules of thumb for sampling:

* Frequent events should be sampled more heavily than rare events, as frequent events tend to
  provide less "signal" per event.
* Success events (e.g. `200 OK`) should be sampled more heavily than error events (e.g. `400 Bad
  Request`), as error events tend to provide more urgently needed information.
* Events closer to your business logic (e.g. paying customer transactions) should be sampled less
  heavily, as they tend to provide more insight into "bottom line" questions than events related to
  general system behavior.

### Basic approaches to sampling

Sampling approaches fall into two broad categories: constant and dynamic.

#### Constant sampling

**Constant sampling** applies a fixed sampling rate to a bucket, with no sensitivity to traffic
patterns. If the bucket sees 1 event per minute and the rate then spikes to 1,000 events per
minute, the rate doesn't change. This is what the `sample` transform currently provides.

#### Dynamic sampling

**Dynamic sampling** involves adjusting the sampling rate based on the frequency of events. This
approach takes into account the "history" of the event stream inside of a specified time window.
Let's say that you have a bucket for `warning`-level logs that you're sampling only very lightly.
If that bucket suddenly gets hammered with traffic, most use cases  call for increasing the sample
rate to avoid **oversampling**, which means retaining more events than you need to satisfy your use
case. The `sample` transform doesn't currently provide this.

#### Example scenario

You're a sysadmin for a major online publication that serves tons of static content using nginx. You
apply constant sampling to HTTP failure logs (HTTP `500s`) at a rate of 1/10. An article that your
publication is hosting jumps to the top of Hacker News and gets inundated with traffic. Your servers
are struggling to handle the load and throwing tons of `500`s. Under normal conditions, your servers
throw only one `500` a second but now they're throwing *10,000* a second. With the 1/10 sampling
rate applied, that means that you're now seeing 1,000 HTTP `500`s per second, a massive jump.

The problem is that you don't *need* to get 1,000 failure logs per second to know that there's a
serious problem. Your threshold for taking action is only 10 `500`s per second, at which point you
spin up another server instance, re-route to a different load balancer, etc. Anything about 10
`500`s per second is just noise that will cost you money when those logs are shipped to $BIGCORP for
realtime alerting and analysis.

Dynamic sampling would enable you to get the insight you need here—be alerted when the threshold of
`500`s per second is reached—without oversampling.

## Internal Proposal

I propose two major changes to the `sample` transform:

1. Users should be able to create [named](#named-buckets) and [dynamic](#dynamic-buckets) sampling
  buckets. Named buckets are to be created using VRL conditions, dynamic buckets via a proposed
  configuration parameter.
2. Users should be able to assign [sampling behavior](#sampling-behavior) to event buckets,
  including dynamic sampling behavior.

I also propose related changes to the [metric output](#metrics) of the transform.

> At the moment, bucketing technically *is* possible using the `route` and `sample` transforms
> together. In [Appendix A](#appendix-a), I show why this provides a sub-optimal experience.

### Named buckets

**Named buckets** are defined using VRL conditions. All events that match the condition are placed
in the bucket when processed. Here's an example configuration with two named buckets:

```toml
[transforms.sample_customer_data]
type = "sample"

[[bucket.less_interesting_transactions]]
condition = """
.status == 200 && \
  !includes(["pro", "enterprise", "deluxe"], .user.plan) && \
  .transaction.accepted == true
"""
rate = 100

[[bucket.very_interesting_because_catastrophic_transactions]]
condition = """
.status == 500 && \
  .user.plan == "deluxe" && \
  len(.shopping_cart.items) > 10
"""
rate = 1
```

### Dynamic buckets

With named buckets you can dictate precisely which buckets you want in advance. There are cases,
though, when you can't know in advance how many buckets you'll need because that number depends on
what your event stream looks like. This is where **dynamic buckets** come into play (this term isn't
a neologism but I have not encountered it in the observability space).

The key-based sampling that's already supported by the `sample` transform implicitly provides
dynamic bucketing, as each newly encountered value for `key_field` creates a new "bucket." All
events in that bucket, e.g. all events with a `status_code` of `500`, are sampled at `rate`.

Here, I propose to enhance dynamic bucketing by enabling **multi-key sampling**. Instead of a single
`key_field` you can provide several using a `key_fields` list. Events with matching values across
all of the specified fields would be placed in the same bucket. A single key provides only one
"axis" for content-based bucketing.

> The `key_field` option would remain valid but could be deprecated in favor of specifying only one
> key using `key_fields`, thereby making a single key field a special case of key-based sampling.

With this configuration...

```toml
key_fields = ["username", "status"]
```

...these two events would end up in the same bucket...

```json
{
  "status": 200,
  "username": "hoverbear",
  "action": "purchase",
  "transaction": {
    "id": "a1b2c3d4"
  }
}
{
  "status": 200,
  "username": "hoverbear",
  "action": "delete_account",
  "transaction": {
    "id": "z9y8x7"
  }
}
```

...whereas these two would end up in different buckets despite the `username` matching:

```json
{
  "status": 200,
  "username": "hoverbear",
  "action": "purchase",
  "transaction": {
    "id": "a1b2c3d4"
  }
}
{
  "status": 500,
  "username": "hoverbear",
  "action": "delete_account",
  "transaction": {
    "id": "4d3c2b1a"
  }
}
```


#### Order of application

The `sample` transform should place events in buckets in the order in which the conditions are
specified, much like a `switch` statement in many programming languages. An event ends up
in the first bucket whose condition it matches; when a match occurs, condition checking ceases.

This raises the question of fallthrough behavior, i.e. what happens if an event doesn't meet any
condition and thus doesn't fall into a named bucket. I propose a component-level `fallthrough`
parameter with two possible values:

* `drop` means that unbucketed events are sampled at a rate of 1 and thus disappear from the stream
* `keep` means that unbucketed events aren't sampled

An alternative would be treating unbucketed events as belonging to an implicit bucket that can
itself be configured. I submit, however, that this would be sub-optimal, as Remap already enables
you to specify a named fallthrough bucket:

```toml
# Captures all unbucketed events
[[bucket.fallthrough]]
condition = """
true
"""
```

If no fallthrough behavior is specified, I propose `keep` as the default behavior, as dropping
unbucketed events by default is more likely to produce unexpected data loss for users (which is here
presumed to be a worse outcome than e.g. cost overruns).

Please note that there is a related discussion in issue [4644]. Changes to Vector made in response
to that discussion may change the recommendation here.

#### Bucket explosion

Dynamic buckets bear the risk of a species of high cardinality problem. If you specify, say, 4
fields for `key_combination` and each of those fields can have many different values, you may end up
with a great many buckets and thereby high memory usage, performance degradation, etc. Controlling
bucket explosion via setting a hard limit, however, bears the downside that it's not clear which
bucketing strategy should take over if too many buckets are being created. Thus, for a first
iteration I propose adding [metrics](#metrics) to the `sampler` transform that would enable users to
keep track of bucket creation behavior to inform their decisions but not providing an explicit
lever.

#### Named + dynamic?

It's not inconceivable that named and dynamic buckets could coexist for the same event stream. It's
not clear, however, that this behavior serves any particular use case, and thus I propose initially
allowing for either named or dynamic buckets but not both. If users need to apply both approaches to
an event stream, they should use swimlanes to split the stream and apply separate `sampler`s.

### Exclusion

The `sample` transform currently enables you to define criteria for excluding events from being
sampled at all. I propose retaining this option.

### Bucket behavior

With events separated into buckets you can begin specifying *how* events in those buckets are
sampled. The sections below propose per-bucket parameters. All of these parameters can apply to
named *or* dynamic buckets. For named buckets, these parameters are set with the bucket definition;
for dynamic buckets, the parameters would be set at the transform level and apply to all created
buckets. Here's an example configuration:

```toml
[transforms.sample]
type = "sample"
inputs = ["logs"]

# Constant sampling
[[bucket.success]]
condition = ".status >= 200 && .status < 300"
rate = 100

# Dynamic sampling
[[bucket.failure]]
condition = ".status >= 400"
rate = 10
window_secs = 30
sensitivity = 0.7
min_event_threshold = 1000
```

#### `rate`

The base sampling rate for the bucket. If only this parameter is specified, the bucket is constantly
sampled at a rate of 1/N.

#### `sensitivity`

If this parameter is specified, that means that a dynamic sampling algorithm is applied to the
bucket. This determines how aggressively Vector adjusts the sample rate, using `rate` as the
baseline, in light of changes in event frequency. A sensitivity close to 0 would mean very small
increases to the rate, whereas a sensitivity close to 1 would mean much more dramatic adjustments.
[Adaptive Request Concurrency][arc]'s `decrease_ratio` parameter serves as a rough analogy.

To compare with another system in the space, [Cribl] provides two discrete options for dynamic
sampling:

1. **Logarithmic** — The sample rate of the current period is the natural log of the number of
  events in the prior period. 10,000 events in the prior period means a rate of 9.21 in the current
  period (i.e. 1 in 9.21 events is retained).
2. **Square root** — The sample rate of the current period is the square root of the number of
  events in the prior period. 10,000 events in the prior period means a rate of 100 in the current
  period (i.e. 1 in 100 events is retained). This option samples more aggressively than the
  logarithmic option.

The sensitivity concept I'm proposing here provides much more granularity than Cribl's discrete
approach. For a frequency of 10,000 events in a given period, a user should be able to

#### `max_rate` / `min_rate`

> Only applicable if `sensitivity` is defined.

Optional parameters to keep the sampling rate within hard limits.

#### `min_event_threshold`

> Only applicable if `sensitivity` is defined.

The minimum number of events needed, within the `window_secs` time window, to begin adjusting the
sample rate. Below this threshold, `rate` applies. Defaults to 0, meaning no threshold.

#### `window_secs`

This specifies the length of the time window, in seconds, during which the sampling rate is
calculated. Rates can't can be calculated dynamically without a window. I propose 15 seconds as a
default but this can be hashed out during the development process.

### Metrics

I propose gathering the following additional metrics from the `sampler` transform:

* A gauge for the total number of current buckets in play
* A counter for the total number of buckets created

## Doc-level Proposal

More robust dynamic sampling capabilities would require the following documentation changes:

* Either update the existing `sampler` transform docs to include the new configuration parameters
  plus some additional explanation.
* A new dynamic sampling guide that walks through specific sampling use cases.
* A new "Under the Hood" page à la the one we provide for Adaptive Request Concurrency that explains
  the feature in more detail, with accompanying diagrams

## Prior Art

Other systems in the observability space do provide dynamic sampling capabilities. Their feature
sets and user interfaces can provide inspiration for dynamic sampling in Vector (and have influenced
this RFC). In terms of implementation, however, I'm unaware of a system built in Rust that provides
dynamic sampling capabilities, and a survey of the [Crates] ecosystem hasn't yielded any promising
libraries.

## Drawbacks

On the development side, providing more robust dynamic sampling would be far less trivial than
adding a new sink for a service provided by $CLOUD_PLATFORM or a new VRL function but also less
labor intensive than the [ARC] feature.

On the user side, the drawbacks would be largely cognitive. Concepts like bucketing and dynamic
sampling behavior are less intuitive than some others in the observability realm. This is an area
where good documentation and a careful approach to terminology and developer experience would be
extremely important.

## Plan Of Attack

TBD

<a id="appendix-a"></a>
## Appendix A: bucketing using `route`

It should be noted here that a form of bucketing is already possible using [routes], which can
divide events into separate streams based on their content. Currently, you can put a `sample`
transform downstream from a `route` transform and select a `rate`, `key_field`, etc. for each route
that you create. Here's an example:

```toml
[transforms.router]
type = "route"
inputs = ["logs"]
route.success = ".status >= 200 && .status < 300"
route.failure = ".status >= 400"

[transforms.sample_success]
type = "sample"
input = ["router.success"]
rate = 100

[transforms.sample_failure]
type = "sample"
input = ["router.failure"]
rate = 5
```

This isn't a terrible solution, but requiring two different transforms to address bread-and-butter
sampling use cases provides a sub-optimal user experience. Vector users should be able to define
buckets *and* sampling properties within a single configuration block, like so:

```toml
[transforms.sample_http]
type = "sample"
inputs = ["logs"]

[[bucket.success]]
condition = ".status >= 200 && .status < 300"
rate = 100

[[bucket.failure]]
condition = ".status >= 400"
rate = 5
```

[4644]: https://github.com/timberio/vector/issues/4644
[arc]: https://vector.dev/blog/adaptive-request-concurrency
[crates]: https://crates.io
[cribl]: https://docs.cribl.io/docs/dynamic-sampling-function
[dynamic_sampling]: #dynamic-sampling
[exclude]: https://vector.dev/docs/reference/transforms/sample/#exclude
[key_field]: https://vector.dev/docs/reference/transforms/sample/#key_field
[rate]: https://vector.dev/docs/reference/transforms/sample/#rate
[sample]: https://vector.dev/docs/reference/transforms/sample
[route]: https://vector.dev/docs/reference/transforms/route
