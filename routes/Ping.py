import falcon
from constants import HEALTHCHECK_HASHKEY, META_INFO_HASHKEY
from services.redis import redis_cli

def convert_bytestring_keyvalue(data):
    """
    convert {b'':b''} to {'': ''}
    """
    if data is not None:
        return {
            key.decode(): val.decode() for key, val in data.items()
        }
    return {}

class Ping:
    def on_get(self, request, response):
        response.status = falcon.HTTP_200
        response.body = 'THE NORTH REMEMBERS...'

class ConsumerMeta:
    def on_get(self, request, response):
        response.status = falcon.HTTP_200
        response.body = 'THE NORTH REMEMBERS...'

class HealthCheck:
    def on_get(self, request, response):
        health_check_info = convert_bytestring_keyvalue(
            redis_cli.get_all_hash_key_value(HEALTHCHECK_HASHKEY)
        )
        healthy_count = 0
        unhealthy_count = 0

        for info in health_check_info.values():
            if int(info) == 1:
                healthy_count += 1
            else:
                unhealthy_count += 1
        
        healthy_data = dict(
            healthy_count=healthy_count,
            unhealthy_count=unhealthy_count
        )
        response.status = falcon.HTTP_200
        response.body = healthy_data
