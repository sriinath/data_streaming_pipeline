from kafka import KafkaConsumer
from time import sleep
import json

from constants import INTERNAL_CONSUMER_TOPIC, MAX_POLL_DELAY, MIN_POLL_DELAY, MAX_RECORDS_PER_POLL

class Consumer:
    def __init__(self, *args, **kwargs):
        self.__consumer = KafkaConsumer(*args, **kwargs)
        self.__enable_polling = True
        self.__is_active = True
        self.__poll_delay = MIN_POLL_DELAY
        self.__flight_messages = 0

    def is_active(self):
        return self.__is_active

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
        self.__consumer.subscribe(topics=topics)
        print('subscribed to the topics', topics)

    def consume_messages(self, process_fn):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        for message in self.__consumer:
            process_fn(message)
        self.close_consumer()

    def __poll(self, timeout_ms=0, max_records=MAX_RECORDS_PER_POLL):
        while self.__enable_polling:
            try:
                message = self.__consumer.poll(
                    timeout_ms=timeout_ms, max_records=max_records
                )
                self.__flight_messages = 0
                if message:
                    self.__poll_delay = MIN_POLL_DELAY
                    max_offset_position = self.get_end_offset(message.keys())
                    for partitions in message:
                        try:
                            records = message.get(partitions)
                            self.__flight_messages += (max_offset_position.get(partitions) \
                                - self.get_current_position(partitions))
                            print(self.__flight_messages)
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
                sleep(self.__poll_delay)
            except AssertionError as assertion_exc:
                print(assertion_exc)
                self.stop_polling()

    def poll_topics(self, process_fn, timeout_ms=0, max_records=None):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        for message in self.__poll(timeout_ms=timeout_ms, max_records=max_records):
            process_fn(message)

    def stop_polling(self):
        print('stop polling and closing the consumer')
        self.__enable_polling = False
        self.close_consumer()

    def close_consumer(self):
        self.__consumer.close()
        self.__is_active = False
