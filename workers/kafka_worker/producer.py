import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class Producer:
    def __init__(self, **kwargs):
        self.__producer = None
        try:
            self.__producer = KafkaProducer(**kwargs)
        except NoBrokersAvailable as exc:
            print(exc)
            print('Please make sure the Kafka brokers are available', kwargs)
    
    def send_message(self, topic, **kwargs):
        if not self.__producer:
            raise Exception('Producer is not yet initiated')

        data = []
        if 'data' in kwargs:
            data = kwargs.pop('data', [])

        if isinstance(data, list) and data:
            for chunk in data:
                key = chunk.get('key', '')
                value = chunk.get('value', {})
                self.__producer.send(
                    topic,
                    key=key,
                    value=value,
                    **kwargs
                )
        else:
            self.__producer.send(topic, **kwargs)

        self.__producer.flush()

    def is_connected(self):
        return self.__producer and self.__producer.bootstrap_connected()
