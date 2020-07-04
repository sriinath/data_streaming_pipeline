import os
import sys
my_path = os.getcwd()
sys.path.insert(0, os.path.join(my_path, '..'))

from workers.worker_pool import DEFAULT_WORKER_THREAD
from workers.kafka_worker.producer import default_producer
from workers.kafka_worker.consumer import default_consumer, internal_consumer
from constants import INTERNAL_CONSUMER_TOPIC, SUBSCRIBE_TOPIC, CONSUME_MESSAGE

def default_processor(message):
    print(message)
    default_producer.send_message(
        INTERNAL_CONSUMER_TOPIC, key=CONSUME_MESSAGE, value={
            '_internal_topic': message.topic,
            '_internal_key': str(message.key),
            '_internal_value': message.value
        }
    )
    print('pushed message into the internal consumer topic')

def internal_processor(message):
    value = message.value
    internal_task_key = message.key
    if internal_task_key == CONSUME_MESSAGE.encode():
        print('consuming message for internal process')
        print(value)

def run_manager():
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
