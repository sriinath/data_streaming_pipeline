import os
import sys

from workers.worker_pool import DEFAULT_WORKER_THREAD
from workers.kafka_worker.producer import default_producer
from workers.kafka_worker.consumer import default_consumer, internal_consumer
from constants import INTERNAL_CONSUMER_TOPIC, SUBSCRIBE_TOPIC, CONSUME_MESSAGE

def default_processor(message):
    default_producer.send_message(INTERNAL_CONSUMER_TOPIC, key=CONSUME_MESSAGE, value=message.value)

def internal_processor(message):
    print('in internal internal_processor')
    value = message.value
    internal_task_key = message.key
    interval_task_data = value.get('value', '')
    if internal_task_key == SUBSCRIBE_TOPIC.encode():
        default_consumer.subscribe_topics(interval_task_data)
    else:
        print('something internal')

def run_processes():
    print('Starting the processor in worker for consuming default messages')
    DEFAULT_WORKER_THREAD.process_tasks(
        default_consumer.poll_topics, default_processor
    )
    print('Successfully added the task for consuming default message')
    print('Starting the processor in worker for consuming internal messages')
    DEFAULT_WORKER_THREAD.process_tasks(
        internal_consumer.poll_topics, internal_processor
    )
    print('Successfully added the task for consuming internal message')
