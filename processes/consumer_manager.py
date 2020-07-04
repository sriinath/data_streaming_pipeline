from workers.kafka_worker.consumer import Consumer
from constants import MAX_MESSAGES_PER_CONSUMER, MAX_CONSUMERS

class KafkaConsumerManager:
    def __init__(self, topics=list(), consumer_configs={}, **kwargs):
        self.__topics = topics
        self.__consumer_configs = consumer_configs 
        self.__default_consumer = Consumer(**consumer_configs)
        self.__consumer_list = list()
        self.__max_consumers = int(kwargs.get('max_consumers', MAX_CONSUMERS))
        self.__max_messages_per_consumer = int(kwargs.get('max_message', MAX_MESSAGES_PER_CONSUMER))

    def add_topic(self, topic):
        if topic not in self.__topics:
            self.__topics.append(topic)

    def remove_topic(self, topic):
        if topic in self.__topics:
            self.__topics.remove(topic)

    def subscribed_topics(self):
        return self.__topics

    def get_consumer_count(self):
        return len(self.__consumer_list)

    def increment_consumer(self, count):
        assert isinstance(count, int), 'count must be a integer'
        assert count <= self.__max_consumers, 'Maximum consumers spawned and cannot increment consumers'
        for _ in range(count):
            self.__consumer_list.append(
                Consumer(**self.__consumer_configs)
            )
    
    def decrement_consumer(self, count):
        assert isinstance(count, int), 'count must be a integer'
        for consumer in self.__consumer_list[:count]:
            consumer.stop_polling()
            self.__consumer_list.pop()

    def get_messages_in_flight(self):
        return self.__default_consumer.get_messages_in_flight()
