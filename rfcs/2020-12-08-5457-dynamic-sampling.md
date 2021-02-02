# RFC 5457 - <2020-12-08> - More robust sampling using the `sample` transform

Vector currently provides some limited sampling capabilities via the [`sample`][sample] transform. But this transform has some fundamental limitations that make it less robust than some other observability platforms:

* The ability to create sampling "buckets" is limited to matching on the value of a specific key using  [`key_field`][key_field].
* You can only apply **constant sampling** to buckets, i.e. the [`rate`][rate] that you set remains fixed at 1/N, with no regard for event traffic patterns.

In this RFC I propose two fundamental updates to the `sample` transform:

1. The ability to create sampling buckets in a much more fine-grained way. This includes two new possibilities:
  1. Using Vector Remap Language comparison expressions to create buckets based on conditions (e.g. `.status == 200 && .level == "warning"`).
  2. Allow for key-based sampling across multiple fields rather than just one, as with `key_field`.
2. The addition of [**dynamic sampling**](#dynamic-sampling) capabilities. Dynamic sampling adjusts the sample rate based on traffic patterns, decreasing the sampling rate when events arrive in a bucket more frequently

The addition of these capabilities to the transform would enable us to cover a much broader range of sampling use cases than currently.

## Scope

This RFC proposes improvements to the [`sample`][sample] transform, including new configuration parameters and the introduction of the **bucket** concept to Vector. These changes would be fully encapsulated at the transform level and needn't have any impact on Vector's core pipeline or any other components. It also wouldn't require any changes to Vector Remap Language, though we should remain open to adding sampling-specific functions should the need or demand arise.

### Non-goals

The purpose of the RFC is to achieve consensus around a set of desired features. Thus, the RFC does *not* cover:

* Any Vector internals that would affect other components, e.g. the processing pipeline or configuration system
* Specific implementation details on the Rust side
* Finalized names for configuration parameters, which inevitably undergo change during the development process

### Domains

Improved sampling has potential implications for both logging and tracing. It does *not* have implications for metrics, as metrics are already a form of sampling in themselves (e.g. counters reveal a raw number of events while saying nothing about the content of those events).

This RFC, however, only covers logging. The tracing domain is different enough that any overlap between the improvements proposed here and tracing would be little more than happy accidents. Log events, for example, can be treated as isolated atoms, whereas traces by definition consist of spans united by a common identifier. When Vector moves into tracing, we'll need to assume that the sampling question needs to be taken up afresh.

### Feature parity

To my knowledge, this RFC doesn't propose any features that are not available in other systems in the observability space, such as [Cribl]. The goal here is more to bring Vector in line with what I take to be the state of the art than to push Vector into new territory.

## Background

Sampling is the art of retaining only a subset of an event stream based on supplied criteria and omitting the rest. Omitting events is crucial to pursuing goals like cost cutting. But sampling needs to be done with care lest it compromise the *insight* that your data streams can provide. Fortunately, if undertaken intelligently and with the right tools, sampling lets you have your cake 🍰 (send fewer events downstream) and eat it too 🍴 (get the insight you need into your running systems).

Some common rules of thumb for sampling:

* Frequent events should be sampled more heavily than rare events, as frequent events tend to provide less information per event.
* Success events (e.g. `200 OK`) should be sampled more heavily than error events (e.g. `400 Bad Request`), as error events tend to provide more urgently needed information.
* Events closer to your business logic (e.g. paying customer transactions) should be sampled less heavily, as they provide more insight into "bottom line" questions than events related to system behavior.

Correspondingly, any robust sampling system should provide the ability to:

* Put events in desired "buckets" based on arbitrarily complex criteria.
* Define the sampling behavior of the resulting buckets in a granular way.

### General approaches to sampling

Sampling approaches fall into two broad categories:

* **Constant sampling** applies a fixed sampling rate to a bucket, with regard for neither the content of events nor their frequency. This is what the `sample` transform currently provides.
* **Dynamic sampling** involves adjusting the sampling rate based on the frequency of events. This approach takes into account the "history" of the event stream inside of a specified time window. You might care enough about, say, `warning`-level logs to sample them only very lightly. But if `warning` events begin to spike, you may want to increase the sample rate to avoid oversampling, i.e. retaining far more events than you need to achieve the desired level of insight.

#### Example scenario

You're a sysadmin for a major online publication that serves tons of static content using nginx. You apply constant sampling to HTTP failure logs (HTTP 500s) at a rate of 1/10. An article that your publication is hosting jumps to the top of Hacker News and gets inundated with traffic. Your servers are struggling to handle the load and throwing tons of 500s. Under normal conditions, your servers throw only one 500 a second. Now, they're throwing 1000 500s a second. With the 1/10 sampling rate applied, that means that you're seeing 100 HTTP 500s per second.

The problem is that you don't need to get 100 failure logs per second to know that there's a serious problem. Your threshold is 10 500s per second to take action (spin up another server instance, re-route to a different load balancer, etc.). Anything above that is just noise that will cost you money when those logs are shipped off to $BIGCORP for realtime alerting and analysis. Dynamic sampling would enable you to get the insight you need here (be alerted when the threshold of 500s per second is reached) without oversampling.

The `sample` transform in its current state would not provide this due to two limitations:

## Motivation

In its current state, the [`sample`][sample] transform provides:

1. Constant sampling, though only implicitly. To achieve constant sampling, you need to specify a [`rate`][rate] but no [`key_field`][key_field]. In that case, all events are hashed the same
  and uniformly sampled at `rate`. This RFC does not propose changing this behavior.
2. Limited dynamic sampling in the form of **key-based** sampling. With the `sampler` you can:
  * Exclude events from sampling based on their actual content using [`exclude`][exclude].
  * Apply a sampling rate on a per-key basis using `key_field`. When making sampling
    decisions, the `sampler` chooses 1 out of N events with the same value for a given key.
    Thus with a `rate` of 5 and a `key_field` of `code` for HTTP status, 1 out of 5 events
    with the code 200 are sampled, 1 out of 5 with code 500, and so on.
  * A combination of exclusion and key-based sampling (with exclusion criteria applied first).

This RFC calls for far more robust dynamic sampling as laid out in the [internal
proposal](#internal-proposal).

### The `route` alternative

It should be noted here that a form of bucketing is already possible using [routes], which can divide events into
separate streams based on their content. Currently, you can put a `sample` transform downstream from a `route` transform
and select a `rate`, `key_field`, etc. for each route that you create. Here's an example:

```toml
[transforms.router]
type = "route"
inputs = ["logs"]
route.success = ".status >= 200 && .status < 300"
route.failure = ".status >= 400"

[transforms.sample_success]
input = ["router.success"]
rate = 100

[transforms.sample_failure]
input = ["router.failure"]
rate = 5
```

This isn't a terrible solution, but requiring two different transforms to address bread-and-butter sampling use cases
provides a sub-optimal user experience. Vector users should be able to define buckets *and* sampling properties within a
single configuration block, like so:

```toml
[transforms.sample_http]
type = "sample"

[[bucket.success]]
condition = ".status >= 200 && .status < 300"
rate = 100

[[bucket.failure]]
condition = ".status >= 400"
rate = 5
```

This method

## Internal Proposal

I propose two major change to the `sampler` transform:

1. User should be able to create [named](#named-buckets) and [dynamic](#dynamic-buckets) sampling
    buckets. Named buckets are to be created using Remap conditions, dynamic buckets via
    configuration.
2. Users should be able to assign [sampling behavior](#sampling-behavior) to event buckets.

I propose related changes to the [metric output](#metrics) of the transform.

### Named buckets

Named buckets are defined using Remap conditions. All events that match the condition are placed
in the bucket when processed. Here's an example configuration with two named buckets:

```toml
[transforms.sample_customer_data]
type = "sampler"

[[bucket.less_interesting_transactions]]
condition = """
.status == 200 && !in_list(.user.plan, ["pro", "enterprise", "deluxe"]) && .transaction.accepted == true
"""
rate = 100

[[bucket.very_interesting_because_catastrophic_transactions]]
condition = """
.status == 500 && .user.plan == "deluxe" && len(.shopping_cart.items) > 10
"""
rate = 1
```

Named buckets are useful because they enable you to route events using arbitrarily complex criteria.
Any Remap expression that returns a Boolean can be used as a condition.

#### Order of application

The `sampler` transform should place events in buckets in the order in which the conditions are
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

### Dynamic buckets

With named buckets you can dictate precisely which buckets you want in advance. There are cases,
though, when you can't know in advance how many buckets you'll need because that number depends on
what your event stream looks like. This is where **dynamic buckets** come into play (this term isn't
a neologism but I have not encountered it in the observability space).

The key-based sampling that's already supported by the `sampler` transform implicitly provides
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

#### Bucket explosion

Dynamic buckets bear the risk of a species of high cardinality problem. If you specify, say, 4
fields for `key_combination` and each of those fields can have many different values, you may end up
with many, many buckets and thereby high memory usage, performance degradation, etc. Controlling
bucket explosion via setting a hard limit, however, bears the downside that it's not clear which
bucketing strategy should take over if too many buckets are being created. Thus, for a first
iteration I propose adding [metrics](#metrics) to the `sampler` transform that would enable users to
keep track of bucket creation behavior to inform their decisions but not providing an explicit
lever.

#### Named + dynamic?

It's not inconceivable that named and dynamic buckets could coexist for the same event stream. It's
not clear, however, that this behavior serves any particular use case, and thus I propose initially
allowing for either named or dynamic buckets but not both. If users need to apply both approaches
to an event stream, they should use swimlanes to split the stream and apply separate `sampler`s.

### Exclusion

The `sampler` transform currently enables you to define criteria for excluding events from being
sampled (i.e. ending up in any bucket). I propose retaining the exclusion option but allowing users
to specifiy criteria via `check_fields` *or Remap* (only `check_fields` is currently available).

### Bucket behavior

With events separated into buckets you can begin specifying *how* events in those buckets are
sampled. The sections below propose per-bucket parameters. All of these parameters can apply to
named *or* dynamic buckets. For named buckets, these parameters are set with the bucket definition;
for dynamic buckets, the parameters would be set at the transform level and apply to all created
buckets.

#### `rate`

The base sampling rate for the bucket. If only this parameter is specified, the bucket is constantly
sampled at a rate of 1/N.

#### `sensitivity`

If this parameter is specified, that means that a dynamic sampling algorithm is applied to the
bucket. This determines how Vector adjusts the sample rate, using `rate` as the baseline, in light
of changes in event frequency. A sensitivity close to 0 would mean only small adjustments whereas a
sensitivity close to 1 would mean more dramatic adjustments.

> [Adaptive Request Concurrency][arc]'s `decrease_ratio` parameter serves as a rough analogy.

The parameters below only make sense if `sensitivity` is defined.

#### `max_rate` / `min_rate`

Optional parameters to keep the sample rate within hard limits.

#### `min_event_threshold`

The minimum number of events needed, within the time window, to begin adjusting the sample rate.
Below this threshold, `rate` applies. Defaults to 0, meaning no threshold.

#### `window_secs`

This specifies the length of the time window, in seconds, during which the sampling rate is
calculated. No dynamic rate can be calculated without a window. I propose 15 seconds as a default
but this can be determined during the development process.

### Metrics

I propose gathering the following additional metrics from the `sampler` transform:

* A gauge for the total number of current buckets
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

On the development side, providing more robust dynamic sampling would be far less trivial than e.g.
adding a new sink for a messaging service provided by `$CLOUD_PLATFORM` or a new Remap function, but
likely less labor intensive than e.g. the Adaptive Request Concurrency feature.

On the user side, the drawbacks would be largely cognitive. Concepts like bucketing and dynamic
sampling behavior are less intuitive than others in the observability realm. This is an area where
good documentation and a careful approach to terminology and developer experience would be extremely
important.

## Plan Of Attack

TBD

[4644]: https://github.com/timberio/vector/issues/4644
[arc]: https://vector.dev/blog/adaptive-request-concurrency
[crates]: https://crates.io
[cribl]: https://docs.cribl.io/docs/dynamic-sampling-function
[exclude]: https://vector.dev/docs/reference/transforms/sample/#exclude
[key_field]: https://vector.dev/docs/reference/transforms/sample/#key_field
[rate]: https://vector.dev/docs/reference/transforms/sample/#rate
[sample]: https://vector.dev/docs/reference/transforms/sample
[route]: https://vector.dev/docs/reference/transforms/route

