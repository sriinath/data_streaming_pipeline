import json
import uuid
from falcon import HTTP_200, HTTP_400, HTTPInternalServerError

from workers.kafka_worker.producer import Producer
from processes.redis_utils import DEFAULT_REDIS
from exceptions.exception_handler import ExceptionHandler
from constants import BROKER_URLS

app_producer = Producer(
    bootstrap_servers=BROKER_URLS,
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class Topics:
    @ExceptionHandler
    def on_put(self, request, response):
        topic_key = request.get_header('topic-key')
        assert topic_key, 'topic-key is a mandatory header to push messages to topic'

        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'

        topic = req_body.get('topic')
        assert topic, 'Topic is mandatory in the request body'

        topic_data = DEFAULT_REDIS.get_hash_value(
            topic_key, topic
        )
        assert topic_data, 'Topic is either not created or the topic-key header is not valid' 

        data = req_body.get('data', [])
        assert data, 'Cannot send an empty message to the topic'

        partition = req_body.get('partition')
        assert isinstance(partition, int) if partition else True, 'partition must be a valid integer'

        headers = [(
            'topic_key', topic_key.encode('utf-8')
        )]

        if isinstance(data, list):
            app_producer.send_message(topic, data=data, headers=headers, partition=partition)
        else:
            app_producer.send_message(topic, key='', value=data, headers=headers, partition=partition)

        response.body = json.dumps({
            'status': 'Success',
            'message': 'Added the message to the topic successfully'
        })
        response.status = HTTP_200

    @ExceptionHandler
    def on_post(self, request, response):
        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'

        topics = req_body.get('topics', [])
        response_data = list()

        for topic in topics:
            topic_hash_key = str(uuid.uuid4())
            response_data.append({
                topic: topic_hash_key
            })

            DEFAULT_REDIS.update_hash_value(
                topic_hash_key, topic, json.dumps([])
            )

        response.body = json.dumps({
            'status': 'Success',
            'message': 'Created topic(s) succesfully',
            'data': response_data
        })
        response.status = HTTP_200
