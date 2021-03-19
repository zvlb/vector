---
title: Monitoring
description: How to monitor and observe Vector with logs, metrics, and more.
---

This document will cover monitoring Vector.

## Logs

### Accessing

<Tabs labels={[
"APT",
"DPKG",
"Docker CLI",
"Docker Compose",
"Homebrew",
"MSI",
"Nix",
"RPM",
"Vector CLI",
"Yum"
]}>
<TabItem index={0}>

The Vector package from the APT repository installs Vector as a Systemd service. Logs can be
accessed through the `journalctl` utility:

```bash
sudo journalctl -fu vector
```

</TabItem>
<TabItem index={1}>

The Vector DEB package installs Vector as a Systemd service. Logs can be
accessed through the `journalctl` utility:

```bash
sudo journalctl -fu vector
```

</TabItem>
<TabItem index={2}>

If you've started Vector through the `docker` CLI you can access Vector's logs
via the `docker logs` command. First, find the Vector container ID:

```bash
docker ps | grep vector
```

Copy Vector's container ID and use it to tail the logs:

```bash
docker logs -f <container-id>
```

</TabItem>
<TabItem index={3}>

If you started Vector through Docker compose you can use the following command
to access Vector's logs:

```bash
docker-compose logs -f vector
```

Replace `vector` with the name of Vector's service if it is not called `vector`.

</TabItem>
<TabItem index={4}>

When Vector is started through Homebrew the logs are automatically routed to
`/usr/local/var/log/vector.log`. You can tail them with the `tail` utility:

```bash
tail -f /usr/local/var/log/vector.log
```

</TabItem>
<TabItem index={5}>

The Vector MSI package does not install Vector into a proces manager. Therefore,
Vector must be started by executing the Vector binary directly. Vector's logs
are written to `STDOUT`. You are in charge of routing `STDOUT`, and this
determines how you access Vector's logs.

</TabItem>
<TabItem index={6}>

The Vector Nix package does not install Vector into a proces manager. Therefore,
Vector must be started by executing the Vector binary directly. Vector's logs
are written to `STDOUT`. You are in charge of routing `STDOUT`, and this
determines how you access Vector's logs.

</TabItem>
<TabItem index={7}>

The Vector RPM package installs Vector as a Systemd service. Logs can be
accessed through the `journalctl` utility:

```bash
sudo journalctl -fu vector
```

</TabItem>
<TabItem index={8}>

If you are starting Vector directly from the Vector CLI then all logs will be
written to `STDOUT`. You are in charge of routing `STDOUT`, and this determines
how you access Vector's logs.

</TabItem>
<TabItem index={9}>

The Vector package from the Yum repository installs Vector as a Systemd service. Logs can be
accessed through the `journalctl` utility:

```bash
sudo journalctl -fu vector
```

</TabItem>
</Tabs>

### Levels

By default, Vector logs on the `info` level, you can change the level through
a variety of methods:

| Method                                       | Description                                                                         |
| :------------------------------------------- | :---------------------------------------------------------------------------------- |
| [`-v` flag][docs.process-management#flags]   | Drops the log level to `debug`.                                                     |
| [`-vv` flag][docs.process-management#flags]  | Drops the log level to `trace`.                                                     |
| [`-q` flag][docs.process-management#flags]   | Raises the log level to `warn`.                                                     |
| [`-qq` flag][docs.process-management#flags]  | Raises the log level to `error`.                                                    |
| [`-qqq` flag][docs.process-management#flags] | Turns logging off.                                                                  |
| `LOG=<level>` env var                        | Set the log level. Must be one of `trace`, `debug`, `info`, `warn`, `error`, `off`. |

### Full Backtraces

You can enable full error backtraces by setting the `RUST_BACKTRACE=full` environment
variable. More on this in the [Troubleshooting guide][guides.advanced.troubleshooting].

### Rate Limiting

Vector rate limits log events in the hot path. This is to your benefit as
it allows you to get granular insight without the risk of saturating IO
and disrupting the service. The trade-off is that repetitive logs will not be
logged.

## Metrics

Currently, Vector does not expose metrics. [Issue #230][urls.issue_230]
represents work to run internal Vector metrics through Vector's pipeline.
Allowing you to define internal metrics as a [source][docs.sources] and
then define one of many metrics [sinks][docs.sinks] to collect those metrics,
just as you would metrics from any other source.

## Troubleshooting

Please refer to our troubleshooting guide:

<Jump to="/docs/setup/guides/troubleshooting">Troubleshooting Guide</Jump>

[docs.process-management#flags]: /docs/administration/process-management/#flags
[docs.sinks]: /docs/reference/sinks/
[docs.sources]: /docs/reference/sources/
[guides.advanced.troubleshooting]: /guides/advanced/troubleshooting/
[urls.issue_230]: https://github.com/timberio/vector/issues/230
