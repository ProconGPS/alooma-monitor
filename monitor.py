import alooma
import datadog
import os
import time
import sys
from datetime import datetime

ALOOMA_USERNAME = os.environ.get('ALOOMA_USERNAME')
ALOOMA_PASSWORD = os.environ.get('ALOOMA_PASSWORD')
ALOOMA_INSTANCE = os.environ.get('ALOOMA_INSTANCE', 'stg').lower()
DATADOG_API_KEY = os.environ.get('DATADOG_API_KEY')
MINUTES_SLEEP = 10
SECONDS_SLEEP = MINUTES_SLEEP * 60

if ALOOMA_INSTANCE.lower() == 'prod':
    alooma_account_name = 'spireon-prod'
elif ALOOMA_INSTANCE.lower() == 'prod-overflow':
    alooma_account_name = 'spireon-prod-overflow'
else:
    alooma_account_name = 'spireon'

api = alooma.Client(
    username=ALOOMA_USERNAME,
    password=ALOOMA_PASSWORD,
    account_name=alooma_account_name
)

datadog.initialize(api_key=DATADOG_API_KEY)


def posix_timestamp():
    d = datetime.now()
    return str(int(time.mktime(d.timetuple())))


def send_metric(data):
    for d in data:
        metric_name = "alooma.{}.{}".format(ALOOMA_INSTANCE, d['target'].lower())
        values = d['datapoints']

        for x in values:
            x.reverse()

        for v in values:
            v[0] = str(v[0])
            if v[1] is not None:
                v[1] = float(v[1])

        values = [tuple(x) for x in values]

        for v in values:
            log("value: {}".format(v))
            if v[1] is not None:
                result = datadog.api.Metric.send(
                    metric=metric_name,
                    points=v,
                    type='gauge',
                )
                yield result
            else:
                log("Value is None. Not sending.")


metrics = alooma.METRICS_LIST
# metrics.remove('CPU_USAGE')
# metrics.remove('MEMORY_CONSUMED')
# metrics.remove('MEMORY_LEFT')


def record_metric(m):
    data = api.get_metrics_by_names(m, MINUTES_SLEEP)
    log("Sending {}".format(m))
    result = send_metric(data)

    for r in result:
        log(r)


def record_all_metrics():
    for m in metrics:
        record_metric(m)


def record_num_inputs():
    inputs = api.get_inputs()
    num_inputs = len(inputs)
    result = datadog.api.Metric.send(
        metric='alooma.{}.num_inputs'.format(ALOOMA_INSTANCE),
        points=[(posix_timestamp(), num_inputs)],
        type='gauge',
    )
    log(result)


def log(message):
    print "[{}]: {}".format(datetime.now().strftime("%Y/%m/%d %H:%M:%S"), message)
    sys.stdout.flush()


if __name__ == '__main__':
    while True:
        try:
            record_all_metrics()
            record_num_inputs()
        except Exception as e:
            log(e)

        log("Sleeping for {}m".format(MINUTES_SLEEP))
        time.sleep(SECONDS_SLEEP)
