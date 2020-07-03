import json
from falcon import HTTP_503
from kafka import KafkaProducer

from exceptions.custom_exception import CustomException

class Producer:
    def __init__(self, **kwargs):
        self.__producer = KafkaProducer(**kwargs)
    
    def send_message(self, *args, **kwargs):
        try:
            self.__producer.send(*args, **kwargs)
            self.__producer.flush()
        except Exception as exc:
            print(exc)

default_producer = Producer(
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
