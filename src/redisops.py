import redis
from functools import wraps

SUCCESS = 0

def handle_exceptions(f):
    @wraps(f)
    def wrapper_func(*args, **kwargs):
        try:
            ret = f(*args, **kwargs)
        except Exception as e:
            print(str(e))
            return None
        return ret
    return wrapper_func

class url_class:
    def __init__(self):
        self.cli = redis.Redis(host='localhost')

    @handle_exceptions
    def seq_write_url_pair(self, o2t_hash, t2o_hash, orig, tiny):
        self.cli.hset(o2t_hash, orig, tiny)
        self.cli.hset(t2o_hash, tiny, orig)
        return SUCCESS

    @handle_exceptions
    def seq_get_tinyurl(self, o2t_hash, orig):
        tiny = self.cli.hget(o2t_hash, orig)
        return tiny

    @handle_exceptions
    def get_origurl(self, t2o_hash, tiny):
        orig = self.cli.hget(t2o_hash, tiny)
        return orig

    @handle_exceptions
    def hash_write_url_mapping(self, t2o_hash, tiny, orig):
        self.cli.hset(t2o_hash, tiny, orig)
        return SUCCESS