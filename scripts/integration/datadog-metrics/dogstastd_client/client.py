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

while(1):
    statsd.increment('foo_metric', tags=['a_tag:1'])
    print("incremented metric")
    time.sleep(5)
