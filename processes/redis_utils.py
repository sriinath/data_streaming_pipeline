import redis

class RedisUtils():
    def __init__(self, **kwargs):
        redis_host = kwargs.get('redis_host', 'localhost')
        redis_port = kwargs.get('redis_port', 6379)
        redis_password = kwargs.get('redis_password', None)
        self.redis_cli = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password
        )

    def get_client(self):
        return self.redis_cli

    def get_hash(self, hash_key):
        try:
            return self.redis_cli.hgetall(hash_key)
        except Exception as redis_err:
            print(redis_err)
        return None

    def get_hash_value(self, hash_key, key):
        try:
            return self.redis_cli.hget(hash_key, key)
        except Exception as redis_err:
            print(redis_err)
        return None

    def update_hash_value(self, hash_key, key, value):
        try:
            self.redis_cli.hset(hash_key, key, value)
        except Exception as redis_err:
            print(redis_err)

    def update_multi_hash_value(self, hash_key, values):
        try:
            self.redis_cli.hmset(hash_key, values)
        except Exception as redis_err:
            print(redis_err)

    def get_value(self, key):
        try:
            return self.redis_cli.get(key)
        except Exception as redis_err:
            print(redis_err)
        return None

    def set_value(self, key, value, ex=None):
        try:
            return self.redis_cli.set(
                key, value, ex=ex
            )
        except Exception as redis_err:
            print(redis_err)
        return None

DEFAULT_REDIS = RedisUtils()
