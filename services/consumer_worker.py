import os
import sys
import ray
sys.path.append(os.getcwd())

from services.consumer_manager import ConsumerManager
from constants import MAX_WORKER_THREADS, MAX_CONSUMER_PROCESS

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
