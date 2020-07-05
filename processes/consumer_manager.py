from threading import active_count

from workers.worker_pool import DEFAULT_WORKER_THREAD
from workers.kafka_worker.consumer import Consumer
from constants import MAX_CONSUMERS

class KafkaConsumerManager:
    def __init__(self, cbk_fn, topics=set(), consumer_configs=dict(), **kwargs):
        self.__topics = topics
        self.__cbk_fn = cbk_fn
        self.__consumer_configs = consumer_configs
        self.__consumer_list = list()
        self.__max_consumers = int(kwargs.get('max_consumers', MAX_CONSUMERS))
        self.__default_consumer = Consumer(*topics, **consumer_configs)
        self.__consume_message(
            self.__default_consumer, self.__cbk_fn
        )

    def __consume_message(self, consumer, process_fn):
        assert isinstance(consumer, Consumer), 'consumer must be an instance of Consumer'
        assert callable(process_fn), 'process_fn must be a callable function'
        assert active_count() <= MAX_CONSUMERS, 'Cannot invoke any more consumers. It has spawned maximum number of avalable consumers'
        DEFAULT_WORKER_THREAD.process_tasks(
            consumer.poll_topics, process_fn
        )

    def subscribed_topics(self):
        return self.__topics

    def get_consumer_count(self):
        return len(self.__consumer_list)

    def get_messages_in_flight(self):
        return self.__default_consumer.get_messages_in_flight()

    def add_topic(self, topic):
        if topic not in self.__topics:
            self.__topics.add(topic)
            self.update_consumer_subscriptions()

    def remove_topic(self, topic):
        if topic in self.__topics:
            self.__topics.remove(topic)
            self.update_consumer_subscriptions()

    def update_consumer_subscriptions(self):
        self.__default_consumer.subscribe_topics(*self.__topics)
        for consumer in self.__consumer_list:
            consumer.subscribe_topics(*self.__topics)

    def increment_consumer(self, count):
        assert isinstance(count, int), 'count must be a integer'
        assert count <= self.__max_consumers, 'Maximum consumers spawned and cannot increment consumers'
        print('spawning new consumers', count)
        for _ in range(count):
            consumer = Consumer(*self.__topics, **self.__consumer_configs)
            self.__consume_message(
                consumer, self.__cbk_fn
            )
            self.__consumer_list.append(consumer)
        print('completed spawning new consumers')
        return self.get_consumer_count()
    
    def decrement_consumer(self, count):
        assert isinstance(count, int), 'count must be a integer'
        print('removing consumers', count)
        for consumer in self.__consumer_list[:count]:
            consumer.stop_polling()
            self.__consumer_list.remove(consumer)
        print('completed removing consumers', count)
        return self.get_consumer_count()

    def check_heart_beat(self):
        print('Heartbeat check')
        for consumer in self.__consumer_list:
            if not consumer.is_active():
                self.__consumer_list.remove(consumer)
        print('Active threads are: ', active_count())
        print('Total Consumer counts: ', self.get_consumer_count())
        print('messages in flight: ', self.get_messages_in_flight())
        print('All Subscription across topics: ', self.subscribed_topics())
        return self.get_consumer_count(), self.get_messages_in_flight()
