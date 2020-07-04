from kafka import KafkaConsumer
from time import sleep
import json

from constants import INTERNAL_CONSUMER_TOPIC, MAX_POLL_DELAY, MIN_POLL_DELAY

class Consumer:
    def __init__(self, *args, **kwargs):
        self.__consumer = KafkaConsumer(*args, **kwargs)
        self.__enable_polling = True
        self.__poll_delay = MIN_POLL_DELAY
        self.__flight_messages = 0

    def get_current_subscriptions(self):
        return self.__consumer.subscription()

    def get_initial_offset(self, partitions):
        return self.__consumer.beginning_offsets(partitions)

    def get_current_position(self, partition):
        return self.__consumer.position(partition)

    def get_end_offset(self, partitions):
        return self.__consumer.end_offsets(partitions)

    def get_messages_in_flight(self):
        return self.__flight_messages

    def subscribe_topics(self, *topics):
        current_topics = self.get_current_subscriptions()
        if current_topics:
            topics = (*topics, *current_topics)
        self.__consumer.subscribe(topics=topics)
        print('subscribed to the topics', topics)

    def consume_messages(self, process_fn):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        for message in self.__consumer:
            process_fn(message)
        self.close_consumer()

    def __poll(self, timeout_ms=0, max_records=None):
        while self.__enable_polling:
            message = self.__consumer.poll(
                timeout_ms=timeout_ms, max_records=max_records
            )
            total_flight_messages = 0
            if message:
                self.__poll_delay = MIN_POLL_DELAY
                total_flight_messages = self.get_end_offset(message.keys())
                for partitions in message:
                    try:
                        records = message.get(partitions)
                        for data in records:
                            yield data
                    except Exception as exc:
                        print(exc)
            else:
                delay = self.__poll_delay * 2
                if delay <= MAX_POLL_DELAY:
                    self.__poll_delay = delay
                else:
                    self.__poll_delay = MAX_POLL_DELAY
            self.__flight_messages = total_flight_messages
            sleep(self.__poll_delay)

    def poll_topics(self, process_fn, timeout_ms=0, max_records=None):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        for message in self.__poll(timeout_ms=timeout_ms, max_records=max_records):
            process_fn(message)

    def stop_polling(self):
        self.__enable_polling = False
        self.close_consumer()

    def close_consumer(self):
        self.__consumer.close()

default_consumer = Consumer(
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='default'
)

internal_consumer = Consumer(
    INTERNAL_CONSUMER_TOPIC,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='internal'
)
