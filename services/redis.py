import os
import redis
import json

class __Redis:
    def __init__(self):
        self.__redis=None

    def connect(self):
        host=os.environ.get('REDIS_URL', 'localhost')
        port=os.environ.get('REDIS_PORT', 6379)
        password=os.environ.get('REDIS_PASSWORD', '')
        if host is not None and port is not None and password is not None:
            self.__redis=redis.Redis(
                host=host,
                port=port, 
                password=password
            )
            try:
                print('Pinging redis connection', self.__redis.ping())
            except Exception as err:
                print('self.__redis is not connected', err)
        else:
            raise Exception('HOST {}, PORT {} and PASSWORD {} should not be NONE for redis connection'.format(host, port, password))

    def get_redis_client(self):
        try:
            if self.__redis is not None:
                if self.__redis.ping():
                    return self.__redis
                else:
                    return None
            else:
                raise Exception("No Active Redis connection available")
        except Exception as err:
            print('self.__redis connection is not valid', err)
            try:
                self.connect()
                if self.__redis is not None and self.__redis.ping():
                    return self.__redis
                else:
                    return None
            except Exception as e:
                print('Exception while connecting redis', e)
                return None
    
    def get_value(self, key):
        cli = self.get_redis_client()
        if cli:
            return cli.get(key)

        return None

    def set_value(self, key, value):
        cli = self.get_redis_client()
        if cli:
            cli.set(key, value)

    def get_multi_value(self, hash_key, key):
        cli = self.get_redis_client()
        if cli:
            return cli.hget(hash_key, key)

        return None

    def get_all_hash_key_value(self, key):
        cli = self.get_redis_client()
        if cli:
            return cli.hgetall(key)

        return None

    def set_multi_value(self, hash_key, key, value):
        cli = self.get_redis_client()
        if cli:
            cli.hset(hash_key, key, value)

redis_cli = __Redis()
