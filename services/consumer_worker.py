import os
import sys
import ray
import requests
sys.path.append(os.getcwd())

from services.consumer_manager import ConsumerManager
from constants import MAX_WORKER_THREADS, MAX_CONSUMER_PROCESS, CALLBACK_HEADERS, \
    CALLBACK_URL, SUBSCRIBER_INFO_URL, SUBSCRIBER_INFO_HEADERS

def create_consumer():
    consumer_processes = list()
    consumer_manager = ConsumerManager.options(
        max_concurrency=MAX_WORKER_THREADS
    ).remote()

    for _ in range(MAX_WORKER_THREADS):
        consumer_processes.append(
            consumer_manager.create_consumer.remote()
        )
    
    return consumer_processes


def callback_on_consumer_initiation():
    if CALLBACK_URL:
        callback_data = dict(
            total_consumer_count=MAX_CONSUMER_PROCESS * MAX_WORKER_THREADS,
            active_consumer_count=ConsumerManager.get_consumer_count(),
            healthcheck=dict(
                url=SUBSCRIBER_INFO_URL,
                headers=SUBSCRIBER_INFO_HEADERS
            )
        )

        resp = requests.post(
            CALLBACK_URL, json=callback_data, headers=CALLBACK_HEADERS
        )
        if not resp.ok:
            print('Callback was not received well', resp.status_code)


if __name__ == "__main__":
    ray.init(num_cpus=MAX_CONSUMER_PROCESS)
    if ray.is_initialized():
        try:
            print('Ray is successfully launched. Initializing and starting kafka consumer managers')
            consumer_processes = list()
            for _ in range(MAX_CONSUMER_PROCESS):
                consumer_processes += create_consumer()

            print('Spawning of the consumer processes is initiated...')
            ray.get(consumer_processes)

            print('No active consumers running...')
        except Exception as exc:
            print(exc)
            print('Something went wrong while trying to connect to the broker')
            ray.shutdown()
    else:
        print('Ray is not yet initialized. -_-')
