import ray
import json
import requests

from workers.kafka_worker.consumer import Consumer
from constants import CONSUMER_TOPICS, GROUP_ID, BROKER_URLS, MAX_WORKER_THREADS, \
    MAX_CONSUMER_PROCESS

@ray.remote
class ConsumerManager(object):
    __active_messages = 0
    __active_consumer = 0
    def __init__(self, **kwargs):
        self.broker_urls = kwargs.get('broker_urls', BROKER_URLS)
        self.group_id = kwargs.get('group_id', GROUP_ID)
        self.topics = kwargs.get('topics', CONSUMER_TOPICS)
    
    def update_messages_count(self, messages):
        ConsumerManager.__active_messages = messages

    def get_messages_count(self):
        return ConsumerManager.__active_messages

    @staticmethod
    def increment_consumer_count():
        ConsumerManager.__active_consumer += 1
    
    @staticmethod
    def get_consumer_count():
        return ConsumerManager.__active_consumer
    
    def create_consumer(self):
        consumer = None
        try:
            if self.topics:
                consumer = Consumer(
                    *self.topics,
                    bootstrap_servers=BROKER_URLS,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id=GROUP_ID
                )
                ConsumerManager.increment_consumer_count()
                print('Number of active consumers:', ConsumerManager.get_consumer_count())
                print('Polling is being started...')
                consumer.poll_topics(self.message_handler)
                print('Polling has completed...')
            else:
                print('Topics are not configured to poll the messages')
        except Exception as exc:
            print('Something went wrong while trying to start polling')
            print(exc)
    
    def message_handler(self, message):
        for record in message:
            topic = record.topic
            key = record.key
            value = record.value

            payload = dict(
                key=str(key),
                value=value,
                topic=topic
            )
            subscribers_list = value.get('subscribers_list', [])
            if not subscribers_list:
                print('No subscription info available for the payload')
            else:
                for subscribers in subscribers_list:
                    subscriber_url = subscribers.get('url')
                    subscriber_headers = subscribers.get('headers', {})
                    if subscriber_url:
                        resp = requests.post(
                            subscriber_url, headers=subscriber_headers,
                            json=payload
                        )
                        if resp.ok:
                            print('Successfully sent the payload to the subscriber', subscribers)
                        else:
                            print('Failed to send the payload to the subscriber', subscribers)
                            print(resp)
                    else:
                        print('No subscription info available for the payload', subscribers)
