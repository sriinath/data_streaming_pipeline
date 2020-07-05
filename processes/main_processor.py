import json
from time import sleep

from .consumer_manager import KafkaConsumerManager
from workers.kafka_worker.producer import default_producer
from workers.worker_pool import DEFAULT_WORKER_THREAD
from constants import INTERNAL_CONSUMER_TOPIC, CONSUME_MESSAGE, HEARTBEAT_INTERVAL, \
    MAX_MESSAGES_PER_CONSUMER

def default_processor(message):
    print('received message from topic', message.topic)
    default_producer.send_message(
        INTERNAL_CONSUMER_TOPIC, key=CONSUME_MESSAGE, value={
            '_internal_topic': message.topic,
            '_internal_key': str(message.key),
            '_internal_value': message.value
        }
    )
    print('pushed message into the internal consumer topic')

def internal_processor(message):
    print('received message fro internal consumption')
    print(message)
    internal_task_key = message.key
    if internal_task_key == CONSUME_MESSAGE.encode():
        print('consuming message for internal process')
        print(message)

def heart_beat(consumer_manager):
    assert isinstance(consumer_manager, KafkaConsumerManager), 'consumer manager must be instance of KafkaConsumerManager'
    while True:
        try:
            consumer_count, messages_in_flight = consumer_manager.check_heart_beat()
            incremental_counter = messages_in_flight / MAX_MESSAGES_PER_CONSUMER
            if int(incremental_counter) > (consumer_count + 1):
                consumer_manager.increment_consumer(incremental_counter - consumer_count)
            sleep(HEARTBEAT_INTERVAL)
        except Exception as exc:
            print(exc)

print('Starting the default worker consumer manager')
default_consumer = KafkaConsumerManager(
    default_processor,
    consumer_configs=dict(
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='default'
    )
)
print('Successfully added default worker consumer manager')
print('Starting the internal worker consumer manager')
internal_consumer = KafkaConsumerManager(
    internal_processor,
    topics=set([INTERNAL_CONSUMER_TOPIC]),
    consumer_configs=dict(
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='internal'
    )
)
print('Successfully added internal worker consumer manager')

def run_manager():
    DEFAULT_WORKER_THREAD.process_tasks(heart_beat, default_consumer)
    DEFAULT_WORKER_THREAD.process_tasks(heart_beat, internal_consumer)
