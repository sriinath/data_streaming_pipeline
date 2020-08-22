import os
import sys, argparse
import json
import requests
from time import sleep
from threading import Thread

sys.path.append(os.getcwd())
from workers.kafka_worker.producer import Producer
from processes.consumer_manager import KafkaConsumerManager
from processes.redis_utils import DEFAULT_REDIS
from constants import HEARTBEAT_INTERVAL, MAX_MESSAGES_PER_CONSUMER, MAX_CONSUMERS, \
    BROKER_URLS, GROUP_ID, INTERNAL_BROKER_URL, INTERNAL_GROUP_ID, INTERNAL_PARTITIONS
from processes.redis_utils import DEFAULT_REDIS

internal_producer = Producer(
    bootstrap_servers=INTERNAL_BROKER_URL,
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
            print('exception in heart beat')
            print(exc)
        sleep(HEARTBEAT_INTERVAL)


def app_processor(records):
    try:
        for message in records:
            topic = str(message.topic)
            key = str(message.key)
            value = str(message.value)
            headers = message.headers
            is_failure = None
            failure_sub_info = []

            if topic and (key or value):
                topic_key = ''
                for header in headers:
                    key, value = header
                    if key == 'failure_topic':
                        is_failure = value
                    elif key == 'topic_key':
                        topic_key = value
                    elif key == 'subscription_info':
                        failure_sub_info = value

                if is_failure:
                    subscription_info = failure_sub_info
                elif topic_key:
                    subscription_info = DEFAULT_REDIS.get_hash_value(
                        topic_key, topic
                    )
                    subscription_info = json.loads(subscription_info) if subscription_info else []

                message_data = dict(
                    topic=topic,
                    key=key,
                    value=value
                )

                for data in subscription_info:
                    url = data.get('subscription_url', '')
                    headers = data.get('headers', {})
                    method = data.get('method', 'post')
                    req = requests.request(
                        method, url, headers=headers, json=message_data
                    )
                    if req.status_code != 200:
                        internal_producer.send_message(
                            topic,
                            key=key,
                            value=value,
                            partition=INTERNAL_PARTITIONS,
                            headers=list(
                                ('failure_topic', b'true'),
                                ('subscription_info', json.dumps([subscription_info]))
                            )
                        )
                        print('Failed to push message to url: ', url)
                        print(req)
                    else:
                        print('successfully pushed')
    except Exception as exc:
        print(exc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', help='Group ID', default=GROUP_ID)
    args = parser.parse_args()
    group_id = args.group

    app_topics = DEFAULT_REDIS.get_value('app_topics')
    app_topics = json.loads(app_topics) if app_topics else list()
    print('Starting the worker consumer manager')
    app_consumer = KafkaConsumerManager(
        app_processor,
        topics=set(app_topics),
        consumer_configs=dict(
            bootstrap_servers=BROKER_URLS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=group_id
        ),
        max_consumers=MAX_CONSUMERS
    )
    internal_consumer = KafkaConsumerManager(
        app_processor,
        topics=set(app_topics),
        consumer_configs=dict(
            bootstrap_servers=INTERNAL_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=INTERNAL_GROUP_ID
        ),
        max_consumers=MAX_CONSUMERS
    )
    t = Thread(target=heart_beat, args=[app_consumer])
    internal = Thread(target=heart_beat, args=[internal_consumer])
    t.start()
    internal.start()
    print('Successfully started worker consumer manager')

    for value in DEFAULT_REDIS.pop_element('topics'):
        if value:
            print(value)
            app_consumer.add_topic([value])
            internal_consumer.add_topic([value])
