import json
from falcon import HTTP_200 

from exceptions.exception_handler import ExceptionHandler
from services.redis import redis_cli

class Subscriber:
    @ExceptionHandler
    def on_post(self, request, response):
        payload = json.load(request.bounded_stream)
        assert payload, 'JSON body is mandatory to process this request'

        subscriber_key = payload.get('subscriber_key')
        assert subscriber_key, 'subscriber_key is mandatory in request body'

        subscriber_list = json.loads(redis_cli.get_value(subscriber_key))
        subscriber_list += payload.get('subscribers', [])

        redis_cli.set_value(subscriber_key, json.dumps(subscriber_list))

        response.body = json.dumps({
            'status': 'Success',
            'message': 'Successfully updated the subscriber list',
            'key': subscriber_key
        })
        response.status = HTTP_200
