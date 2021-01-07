import os
import redis
import json

class __Redis:
    __redis=None
    @staticmethod
    def connect():
        host=os.environ.get('REDIS_URL')
        port=os.environ.get('REDIS_PORT')
        password=os.environ.get('REDIS_PASSWORD', '')
        if host is not None and port is not None and password is not None:
            __Redis.__redis=redis.Redis(
                host=host,
                port=port, 
                password=password
            )
            try:
                print('Pinging redis connection', __Redis.__redis.ping())
            except Exception as err:
                print('__Redis is not connected', err)
        else:
            raise Exception('HOST {}, PORT {} and PASSWORD {} should not be NONE for redis connection'.format(host, port, password))

    @staticmethod
    def get_redis_client():
        try:
            if __Redis.__redis is not None and __Redis.__redis.ping():
                return __Redis.__redis
            else:
                return  None
        except Exception as err:
            print('__Redis connection is not valid', err)
            try:
                __Redis.connect()
                if __Redis.__redis is not None and __Redis.__redis.ping():
                    return __Redis.__redis
                else:
                    return None
            except Exception as e:
                print('Exception while connecting redis', e)
                return None
    
    def get_value(self, key):
        cli = __Redis.get_redis_client()
        if cli:
            return cli.get(key)

        return None

    def set_value(self, key, value):
        cli = __Redis.get_redis_client().set(key, value)


redis_cli = __Redis()
