import json
from time import sleep

from .consumer_manager import KafkaConsumerManager
from workers.kafka_worker.producer import default_producer
from constants import HEARTBEAT_INTERVAL, MAX_MESSAGES_PER_CONSUMER, MAX_CONSUMERS

def default_processor(message):
    print('received message from topic', message.topic)
    print('consuming message for internal process', message)

def heart_beat(consumer_manager):
    assert isinstance(consumer_manager, KafkaConsumerManager), 'consumer manager must be instance of KafkaConsumerManager'
    while True:
        try:
            consumer_count, messages_in_flight = consumer_manager.check_heart_beat()
            incremental_counter = int(messages_in_flight / MAX_MESSAGES_PER_CONSUMER)
            if incremental_counter > consumer_count:
                consumer_manager.increment_consumer(incremental_counter - (consumer_count + 1))
            elif consumer_count > (incremental_counter + 1):
                consumer_manager.decrement_consumer(consumer_count - incremental_counter)
        except Exception as exc:
            print(exc)
        sleep(HEARTBEAT_INTERVAL)

print('Starting the worker consumer manager')
default_consumer = KafkaConsumerManager(
    default_processor,
    consumer_configs=dict(
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='default'
    ),
    max_consumers=MAX_CONSUMERS
)
print('Successfully started worker consumer manager')
