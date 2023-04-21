from datadog import initialize, statsd
import time
import os

STATSD_HOST = os.getenv('STATSD_HOST')

print("initializing for {STATSD_HOST}")

options = {
    'statsd_host':STATSD_HOST,
    'statsd_port':8125
}

initialize(**options)

time.sleep(5)

for i in range(10):
    statsd.increment('foo_metric.rate', tags=['a_tag:1'])
    print("incremented metric")

    statsd.gauge('foo_metric.gauge', i, tags=["a_tag:2"])

    statsd.flush()
    time.sleep(1)
