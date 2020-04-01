import os
import sys
import time
from datetime import datetime

import alooma
import datadog


class AloomaMonitor:
    def __init__(self, alooma_api_key, alooma_instance, datadog_api_key, datadog_app_key,
                 minutes_sleep):
        self.alooma_instance = alooma_instance
        self.datadog_api_key = datadog_api_key
        self.minutes_sleep = minutes_sleep

        if self.alooma_instance.lower() == 'prod':
            self.alooma_account_name = 'spireon-prod'
        elif self.alooma_instance.lower() == 'prod-overflow':
            self.alooma_account_name = 'spireon-prod-overflow'
        else:
            self.alooma_account_name = 'spireon'
        self.alooma_api = alooma.Client(
            api_key=alooma_api_key,
            account_name=self.alooma_account_name
        )
        self.metrics = alooma.METRICS_LIST

        datadog_options = {
            'api_key': datadog_api_key,
            'app_key': datadog_app_key
        }
        datadog.initialize(**datadog_options)

        self.datadog_event_tags = ["environment:{}".format(alooma_instance), 'appgroup:alooma', 'productname:alooma']

    def posix_timestamp(self):
        d = datetime.now()
        return str(int(time.mktime(d.timetuple())))

    def send_metric(self, data):
        for metric in data:
            metric_name = "alooma.{}".format(metric['target'].lower())
            values = metric['datapoints']

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
                        tags=self.datadog_event_tags
                    )
                    yield result
                else:
                    log("Value is None. Not sending.")

    def record_metric(self, m):
        data = self.alooma_api.get_metrics_by_names(m, self.minutes_sleep)
        log("Sending {}".format(m))
        result = self.send_metric(data)

        for r in result:
            log(r)

    def record_all_metrics(self):
        for m in self.metrics:
            self.record_metric(m)

    def record_num_inputs(self):
        inputs = self.alooma_api.get_inputs()
        num_inputs = len(inputs)
        log("inputs: {}".format(num_inputs))
        result = datadog.api.Metric.send(
            metric='alooma.num_inputs'.format(self.alooma_instance),
            points=[(self.posix_timestamp(), num_inputs)],
            type='gauge',
            tags=self.datadog_event_tags
        )
        log(result)


def log(message):
    print "[{}]: {}".format(datetime.now().strftime("%Y/%m/%d %H:%M:%S"), message)
    sys.stdout.flush()


if __name__ == '__main__':
    alooma_api_key = os.environ.get('ALOOMA_API_KEY')
    alooma_instance = os.environ.get('ALOOMA_INSTANCE', 'stg').lower()
    datadog_api_key = os.environ.get('DATADOG_API_KEY')
    datadog_app_key = os.environ.get('DATADOG_APP_KEY')
    minutes_sleep = 10
    seconds_sleep = minutes_sleep * 60
    while True:
        try:
            alooma_monitor = AloomaMonitor(alooma_api_key=alooma_api_key,
                                           alooma_instance=alooma_instance,
                                           datadog_api_key=datadog_api_key,
                                           datadog_app_key=datadog_app_key,
                                           minutes_sleep=minutes_sleep
                                           )

            alooma_monitor.record_all_metrics()
            alooma_monitor.record_num_inputs()
        except Exception as e:
            log(e)
        log("Sleeping for {}m".format(minutes_sleep))
        time.sleep(seconds_sleep)
