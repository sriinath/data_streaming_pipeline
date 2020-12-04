import json
from json.decoder import JSONDecodeError
from falcon import HTTP_400, HTTP_422, HTTP_500

from .custom_exception import CustomException

class ExceptionHandler:
    def __init__(self, func):
        self.func = func

    def __call__(self, request, response, *args, **kwargs):
        try:
            return self.func(self, request, response, *args, **kwargs)
        except CustomException as exc:
            response.status = exc.status_code
            response.body = json.dumps(dict(
                message=exc.message,
                **exc.info
            ))
        except JSONDecodeError as exc:
            print(exc)
            response.status = HTTP_400
            response.body = json.dumps({
                'status': 'Failure',
                'message': 'JSON provided in request body is not valid'
            })
        except AssertionError as exc:
            print(exc)
            message = str(exc)
            if not message:
                message = 'Please make sure the request is valid'
            response.status = HTTP_422
            response.body = json.dumps({
                'status': 'Failure',
                'message': message
            })
        except Exception as exc:
            print(exc)
            response.status = HTTP_500
            response.body = json.dumps({
                'status': 'Failure',
                'message': 'Something went wrong while processing the request'
            })
