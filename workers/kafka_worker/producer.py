from kafka import KafkaProducer
import traceback


class Producer:
    def __init__(self, **kwargs):
        self.__producer = KafkaProducer(**kwargs)
    
    def send_message(self, *args, **kwargs):
        data = []
        if 'data' in kwargs:
            data = kwargs.pop('data')
        try:
            if isinstance(data, list) and data:
                for chunk in data:
                    key = chunk.get('key', '')
                    value = chunk.get('value', {})
                    self.__producer.send(
                        *args,
                        key=key,
                        value=value,
                        **kwargs
                    )
            else:
                self.__producer.send(*args, **kwargs)

            self.__producer.flush()
        except Exception as exc:
            print('Exception while sending message')
            print(exc)
            print(traceback.format_exc())
            raise Exception('Something went wrong while adding message')
