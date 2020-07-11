import json
from falcon import HTTP_200, HTTP_400, HTTPInternalServerError

from workers.kafka_worker.producer import default_producer
from processes.main_processor import default_consumer
from processes.redis_utils import DEFAULT_REDIS
from exceptions.exception_handler import ExceptionHandler
from constants import TOPIC_PREFIX

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
        user_name = request.context['user_name']
        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'

        for subscription in req_body:
            topics = subscription.get('topics', [])
            assert topics, 'Topic is mandatory in the request body'

            subscribe_data = subscription.get('subscibe', [])
            default_consumer.add_topic(topics)
            assert isinstance(subscribe_data, list), 'each item of subscibe must be a object'

            for subscriber in subscribe_data:
                url = subscriber.get('url', '')
                assert url, 'url must be valid'

                for topic in topics:
                    topic_key = '{}:{}'.format(TOPIC_PREFIX, topic)
                    key = '{}:{}'.format(user_name, url)
                    DEFAULT_REDIS.update_hash_value(
                        topic_key, key, json.dumps(subscriber)
                    )

        response.body = json.dumps({
            'status': 'Success',
            'message': 'Subscribed to the topics successfully'
        })
        response.status = HTTP_200
