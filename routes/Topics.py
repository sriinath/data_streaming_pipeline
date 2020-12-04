import json
import uuid
from falcon import HTTP_200 

from exceptions.exception_handler import ExceptionHandler
from workers.kafka_worker.producer import Producer
from constants import BROKER_URLS

app_producer = Producer(
    bootstrap_servers=BROKER_URLS,
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class Topics:
    @ExceptionHandler
    def on_post(self, request, response):
        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'

        topic = req_body.get('topic')
        assert topic, 'Topic is mandatory in the request body'

        data = req_body.get('data', [])
        assert data, 'Cannot send an empty message to the topic'

        partition = request.get_header('partition')
        assert isinstance(partition, int) if partition else True, 'partition must be a valid integer'

        subscribers_list = req_body.get('subscribers', list())
        data['subscribers_list'] = subscribers_list
        if isinstance(data, list):
            app_producer.send_message(
                topic, data=data, partition=partition
            )
        else:
            app_producer.send_message(
                topic, key='', value=data, partition=partition
            )

        response.body = json.dumps({
            'status': 'Success',
            'message': 'Added the message to the topic successfully'
        })
        response.status = HTTP_200
