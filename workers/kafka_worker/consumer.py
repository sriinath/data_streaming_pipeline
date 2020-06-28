from kafka import KafkaConsumer

class Consumer:
    def __init__(self, *args, **kwargs):
        self.__consumer = KafkaConsumer(*args, **kwargs)

    def subscribe_topics(self, *topics):
        self.__consumer.subscribe(topics)

    def consume_messages(self, process_fn):
        assert process_fn and callable(process_fn), \
            'process_fn is mandatory and must be callable'
        print('begin to consume messages')
        for message in self.__consumer:
            print(message)
            process_fn(message)
        print('here')
        self.close_consumer()

    def close_consumer(self):
        self.__consumer.close()
