import json
import requests
from time import sleep

from .consumer_manager import KafkaConsumerManager
from .redis_utils import DEFAULT_REDIS
from workers.kafka_worker.producer import default_producer
from constants import HEARTBEAT_INTERVAL, MAX_MESSAGES_PER_CONSUMER, MAX_CONSUMERS, TOPIC_PREFIX

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

def default_processor(records):
    try:
        for message in records:
            topic = str(message.topic)
            key = str(message.key)
            value = str(message.value)
            if topic and (key or value):
                topic_key = '{}:{}'.format(TOPIC_PREFIX, topic)
                subscription_info = DEFAULT_REDIS.get_hash(
                    topic_key
                )
                message_data = dict(
                    topic=topic,
                    key=key,
                    value=value
                )
                for data in subscription_info.values():
                    data = json.loads(data)
                    url = data.get('url', '')
                    headers = data.get('headers', {})
                    method = data.get('method', 'post')
                    req = requests.request(
                        method, url, headers=headers, json=message_data
                    )
                    if req.status_code != 200:
                        print('Failed to push message to url: ', url)
                        print(req)
                    else:
                        print('successfully pushed')
    except Exception as exc:
        print(exc)

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
