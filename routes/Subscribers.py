import json
from falcon import HTTP_200, HTTP_400, HTTPInternalServerError

from workers.kafka_worker.producer import default_producer
from processes.main_processor import default_consumer
from exceptions.exception_handler import ExceptionHandler

class Subscribers:
    @ExceptionHandler
    def on_put(self, request, response):
        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'
        topic = req_body.get('topic', None)

        assert topic, 'Topic is mandatory in the request body'
        data = req_body.get('data', [])

        assert data, 'Cannot send an empty message to the topic'
        if isinstance(data, list):
            default_producer.send_message(topic, data=data)
        else:
            default_producer.send_message(topic, key='', value=data)
        response.body = json.dumps({
            'status': 'Success',
            'message': 'Added the message to the topic successfully'
        })
        response.status = HTTP_200

    @ExceptionHandler
    def on_post(self, request, response):
        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'
        topic = req_body.get('topic', None)

        assert topic, 'Topic is mandatory in the request body'
        data = req_body.get('data', {})
        print(data)
        default_consumer.add_topic(topic)
        response.body = json.dumps({
            'status': 'Success',
            'message': 'Subscribed to the topic successfully'
        })
        response.status = HTTP_200
