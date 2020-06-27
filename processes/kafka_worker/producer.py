from kafka import KafkaProducer

class Producer:
    def __init__(self, **kwargs):
        self.__producer = KafkaProducer(**kwargs)
    
    def send_message(self, *args, **kwargs):
        self.__producer.send(*args, **kwargs)
