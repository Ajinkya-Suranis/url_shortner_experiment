import redis
from functools import wraps

HASH_T2O = "tiny_to_orig"
HASH_O2T = "orig_to_tiny"
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
    def write_url_pair(self, orig, tiny):
        self.cli.hset(HASH_O2T, orig, tiny)
        self.cli.hset(HASH_T2O, tiny, orig)
        return SUCCESS

    @handle_exceptions
    def get_tinyurl(self, orig):
        tiny = self.cli.hget(HASH_O2T, orig)
        return tiny

    @handle_exceptions
    def get_origurl(self, tiny):
        orig = self.cli.hget(HASH_T2O, tiny)
        return orig