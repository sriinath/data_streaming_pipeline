from kafka import KafkaConsumer
from time import sleep
import json

from constants import MAX_POLL_DELAY, MIN_POLL_DELAY, MAX_RECORDS_PER_POLL, \
    HEALTHCHECK_HASHKEY, META_INFO_HASHKEY
from services.redis import redis_cli

class Consumer:
    __flight_messages = dict()
    __consumers_count = 0

    def __init__(self, *args, **kwargs):
        self.topics = args
        self.consumer_id = kwargs.pop('consumer_id', Consumer.__consumers_count + 1)
        self.manager_id = kwargs.pop('manager_id', '')
        self.__consumer = KafkaConsumer(*args, **kwargs)
        self.__enable_polling = True
        self.__is_active = True
        self.__poll_delay = MIN_POLL_DELAY
        self.__group_id = kwargs.get('group_id', None)
        self.__name = 'Consumer_{}'.format(Consumer.get_consumer_count())
        self.processed_images = 0
        Consumer.increment_consumer_count()
        print('Topics listened by the consumers:', self.topics)
        if self.__group_id and self.__group_id not in Consumer.__flight_messages:
            Consumer.__flight_messages.update({ self.__group_id: 0 })

    @staticmethod
    def get_messages_in_flight():
        return Consumer.__flight_messages

    @staticmethod
    def get_consumer_count():
        return Consumer.__consumers_count
    
    @staticmethod
    def update_consumer_count(count):
        Consumer.__consumers_count = count

    @staticmethod
    def increment_consumer_count():
        Consumer.__consumers_count += 1

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

    def subscribe_topics(self, *topics):
        self.__consumer.subscribe(topics=topics)
        print('subscribed to the topics', topics)

    def consume_messages(self, process_fn):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        for message in self.__consumer:
            process_fn(message)
        self.close_consumer()

    def set_alive(self, is_alive):
        redis_cli.set_multi_value(HEALTHCHECK_HASHKEY, '{}:{}'.format(self.manager_id, self.consumer_id), 1 if is_alive else 0)

    def log_consumer_meta(self):
        consumer_meta = dict(
            messages_count=self.processed_images,
            topics=self.topics,
            group_id=self.__group_id,
            name=self.__name
        )
        redis_cli.set_multi_value(
            META_INFO_HASHKEY, '{}:{}'.format(self.manager_id, self.consumer_id),
            json.loads(consumer_meta)
        )

    def __poll(self, timeout_ms=0, max_records=MAX_RECORDS_PER_POLL):
        while self.__enable_polling:
            print('{} Listening to messages...'.format(self.__name))
            try:
                message = self.__consumer.poll(
                    timeout_ms=timeout_ms, max_records=max_records
                )
                if message:
                    Consumer.__flight_messages[self.__group_id] = 0
                    self.__poll_delay = MIN_POLL_DELAY
                    max_offset_position = self.get_end_offset(message.keys())
                    for partitions in message:
                        try:
                            records = message.get(partitions)
                            Consumer.__flight_messages[self.__group_id] += (max_offset_position.get(partitions) \
                                - self.get_current_position(partitions))
                            yield records
                        except Exception as exc:
                            print(exc)
                else:
                    delay = self.__poll_delay * 2
                    if delay <= MAX_POLL_DELAY:
                        self.__poll_delay = delay
                    else:
                        self.__poll_delay = MAX_POLL_DELAY

                self.processed_images += len(records)
                self.log_consumer_meta()
                self.set_alive(False)
                self.log_consumer_meta()
                sleep(self.__poll_delay)
            except AssertionError as assertion_exc:
                self.stop_polling()
                print(assertion_exc)

        print('returning None')
        self.set_alive(False)
        return None

    def poll_topics(self, process_fn, timeout_ms=0, max_records=MAX_RECORDS_PER_POLL):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        print('polling has begun')
        for records in self.__poll(timeout_ms=timeout_ms, max_records=max_records):
            if records is None:
                break
            process_fn(records)
        print('exit from consuming messages')

    def stop_polling(self):
        print('stopping polling and closing the consumer')
        self.__enable_polling = False
        self.close_consumer()

    def close_consumer(self):
        self.__consumer.close()
        self.__is_active = False
