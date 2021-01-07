import ray
import json
import requests
import aiohttp
import asyncio
from json import JSONDecodeError

from workers.kafka_worker.consumer import Consumer
from constants import CONSUMER_TOPICS, GROUP_ID, BROKER_URLS, MAX_WORKER_THREADS, \
    MAX_CONSUMER_PROCESS
from .redis import redis_cli

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
                consumer.poll_topics(self.consumer_handler)
                print('Polling has completed...')
            else:
                print('Topics are not configured to poll the messages')
        except Exception as exc:
            print('Something went wrong while trying to start polling')
            print(exc)


    def get_available_subscribers(self, subscriber_key):
        subscribers = redis_cli.get_value(subscriber_key)
        if subscribers:
            try:
                return json.loads(subscribers)
            except JSONDecodeError as exc:
                print('Decode exception', exc)
                print(subscribers)
        return []


    async def send_subscriber_data(self, session, subscriber_url, subscriber_headers, payload):
        async with session.post(
            subscriber_url, headers=subscriber_headers,
            json=payload
        ) as resp:
            if resp.ok:
                print('Successfully sent the payload to the subscriber')
            else:
                print('Failed to send the payload to the subscriber')
                print(await resp.text())


    async def process_message(self, record):
        topic = record.topic
        key = record.key
        value = record.value
        headers = record.headers

        payload = dict(
            key=str(key),
            value=value,
            topic=topic
        )

        subscribers_list = value.get('subscribers_list', [])
        for key, value in headers:
            if key == 'subscriber_key':
                subscribers_list += self.get_available_subscribers(value)

        subscriber_tasks = list()
        if not subscribers_list:
            print('No subscription info available for the payload')
        else:
            async with aiohttp.ClientSession() as session:
                for subscribers in subscribers_list:
                    subscriber_url = subscribers.get('url')
                    subscriber_headers = subscribers.get('headers', {})
                    if subscriber_url:
                        subscriber_tasks.append(self.send_subscriber_data(
                            session, subscriber_url, subscriber_headers, payload
                        ))
                    else:
                        print('No subscription info available for the payload', subscribers)
            print('Message for all the Subscribers is being sent')
        await asyncio.gather(*subscriber_tasks)


    async def message_handler(self, message):
        message_tasks = list()
        for record in message:
            message_tasks.append(self.process_message(record))
        
        print('All Message is successfully processed and are being sent to the subscribers')
        await asyncio.gather(*message_tasks)
        print('Message is sent to all the subscibers...')


    def get_async_loop(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as async_loop_error:
            print('There is no existing loop running. Creating a new Loop to execute tasks')
            print(async_loop_error)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop


    def consumer_handler(self, message):
        loop = self.get_async_loop()
        loop.run_until_complete(self.message_handler(message))
