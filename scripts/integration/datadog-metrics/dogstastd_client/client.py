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

for _ in range(1000):
    statsd.increment('foo_metric', tags=['a_tag:1'], 1.0)
    print("incremented metric")
