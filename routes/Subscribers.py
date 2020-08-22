import json
from falcon import HTTP_200, HTTP_400, HTTPInternalServerError

from processes.redis_utils import DEFAULT_REDIS
from exceptions.exception_handler import ExceptionHandler
from processes.redis_utils import DEFAULT_REDIS

class Subscribers:
    @ExceptionHandler
    def on_post(self, request, response):
        topic_key = request.get_header('topic-key')
        assert topic_key, 'topic-key is a mandatory header push message to topic'

        req_body = json.load(request.bounded_stream)
        assert req_body, 'JSON body is mandatory to process this request'

        failed_topic = list()

        for subscription in req_body:
            topics = subscription.get('topics', [])
            assert topics, 'Topic is mandatory in the request body'

            subscribe_data = subscription.get('subscribe', [])
            assert isinstance(subscribe_data, list), 'each item of subscibe must be a object'

            subscription_data = list()
            for subscriber in subscribe_data:
                url = subscriber.get('subscription_url', '')
                assert url, 'url must be valid'
                subscription_data.append(
                    subscriber
                )

            for topic in topics:
                topic_data = DEFAULT_REDIS.get_hash_value(
                    topic_key, topic
                )
                
                if not topic_data:
                    failed_topic.append(topic)
                    topics.remove(topic)
                else:
                    topic_data = json.loads(topic_data) + subscription_data
                    DEFAULT_REDIS.update_hash_value(
                        topic_key, topic, json.dumps(topic_data)
                    )

            assert topics, 'Topics passed must be valid and created before subscribing'
            # app_consumer.add_topic(topics)
            DEFAULT_REDIS.append_element('topics', topics)
        response_data = dict(
            status='Success',
            message='Subscribed to the topics successfully'
        )
        if failed_topic:
            response_data.update(
                status='Failure',
                message='Failed to subscribe to the topics. Make sure these topics are created before subscribing.',
                data=failed_topic
            )

        response.body = json.dumps(response_data)
        response.status = HTTP_200
