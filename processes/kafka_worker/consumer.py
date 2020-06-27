from kafka import KafkaConsumer

class Consumer:
    def __init__(self, *args, **kwargs):
        self.__consumer = KafkaConsumer(*args, **kwargs)

    def subscribe_topics(self, *topics):
        self.__consumer.subscribe(topics)

    def consume_messages(self):
        for message in self.__consumer:
            print(message)
        self.close_consumer()

    def close_consumer(self):
        self.__consumer.close()
