import json
from falcon import HTTPUnauthorized

from .redis_utils import DEFAULT_REDIS

class AuthMiddleware:
    def process_request(self, req, resp):
        print(req.path)
        if req.path == '/ping':
            return

        auth = req.get_header('Authorization')
        if not auth:
            raise HTTPUnauthorized(description='Authorization key in header is mandatory')

        auth_info = DEFAULT_REDIS.get_hash_value('USERS_APIKEY', auth)
        if not auth_info:
            raise HTTPUnauthorized(description='Token passed in header either expired or is not valid')

        auth_info = json.loads(auth_info)
        is_active = auth_info.get('is_active', False)
        if is_active:
            req.context['user_name'] = auth_info.get('user_name', '')
        else:
            raise HTTPUnauthorized(description='Token passed in header either expired or is not valid')



