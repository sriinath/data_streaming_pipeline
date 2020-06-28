from falcon import HTTP_503
from kafka import KafkaProducer

from exceptions.custom_exception import CustomException

class Producer:
    def __init__(self, **kwargs):
        self.__producer = KafkaProducer(**kwargs)
    
    def send_message(self, *args, **kwargs):
        if self.__producer.bootstrap_connected():
            self.__producer.send(*args, **kwargs)
            self.__producer.flush()
            return
        raise CustomException(HTTP_503, 'Kafka connection error')
