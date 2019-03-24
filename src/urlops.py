import random
import os
import pymongo
import hashlib
import base64
from char_mapping import url_char_mapping
from permutations import gen_next_permutation

# 26 lower + 26 upper + 10 numbers = 62 letters
TOTAL_CHARS = 62
STR_LEN = 6
HASH_T2O = "tiny_to_orig"
HASH_O2T = "orig_to_tiny"

# Current, updated set of letters.
current_letters = []
orig_letters = []

cli = pymongo.MongoClient("mongodb://localhost:27017/")
db = cli["urldb"]
col = db["urlcol"]


# get the hash of original URL.
# We rely on sha256 hash algorithm to get a 
# 256-bit i.e. 32 byte hash and again get
# its base64 encoded value.
# But we just consider first 6 bytes of that hash.
# Because of avalanche effect of sha256 hash, we get
# a pretty good probability that no two different URLs
# would result into same first 6-byte hash encoding.

def get_url_hash(origurl):
    m = hashlib.sha256()
    m.update(bytes(origurl))
    s = base64.b64encode(m.digest())
    hashval = s[:6]
    if '/' in hashval:
        hashval = hashval.replace("/", "-")
    return hashval


# create random combination of 6 letters.
# This is called only when the service
# is being deployed for the first time.
def create_random_letters(col):
    global current_letters, orig_letters
    random_numbers = []
    for _i in range(0, 6):
        random_numbers.append(random.randint(1, TOTAL_CHARS - 1))
    current_letters = random_numbers
    orig_letters = current_letters[:]
    doc = {"current_letters": random_numbers}
    col.insert_one(doc)
    return doc


# Create the json object for 6 letters of tiny url.
# Check if the document already exists.
# If not, create the json and write into collection.
# If it already exists, return it.
def create_tinyurl_letters():
    global current_letters, orig_letters, col
    result = col.find({"current_letters": {"$exists": 1}})
    if result.count() > 0:
        # already created. just return
        for doc in result:
            current_letters = doc["current_letters"]
            orig_letters = current_letters[:]
            return doc["current_letters"]
    return create_random_letters(col)


# function to get the next permutation of 6 letter
# characters as json object. input is mongodb
# collection object. It assumes that the
# document already exists in mongodb.
def get_tinyurl_string(algo, redis_cli, url):
    global current_letters, orig_letters, TOTAL_CHARS, STR_LEN, col
    url_string = ''
    if algo == "hash":
        url_string = get_url_hash(url)
        redis_cli.hash_write_url_mapping(HASH_T2O, url_string, url)
    else:
        prev_letters = current_letters[:]
        current_letters = gen_next_permutation(prev_letters, orig_letters,
                                                TOTAL_CHARS, STR_LEN)
        col.update_one({"current_letters": prev_letters},
                        {"$set": {"current_letters": current_letters}})
        for i in current_letters:
            url_string += url_char_mapping[i]
        redis_cli.seq_write_url_pair(HASH_O2T, HASH_T2O, url, url_string)
    col.insert_one({"orig": url, "tiny": url_string})
    return url_string

# Search if the original url(long one) already exists
# in mongodb. If so, return corresponding tiny url,
# else return None.

def search_origurl(algo, redis_cli, origurl):
    global col
    if algo == "sequential":
        result = redis_cli.seq_get_tinyurl(HASH_O2T, origurl)
    else:
        url_hash = get_url_hash(origurl)
        result = redis_cli.get_origurl(HASH_T2O, url_hash)
    if result:
        print("Found in redis!")
        return url_hash
    result = col.find_one({"orig": {"$eq": origurl}})
    if result == None:
        return None
    # Since we've used find_one() above for querying,
    # there is only one item in the result.
    return result["tiny"]


# Search if the tiny URL is already present
# in mongodb. Return the original URL if
# exists, None if not.
def search_tinyurl(redis_cli, tinyurl):
    global col
    orig_url = redis_cli.get_origurl(HASH_T2O, tinyurl)
    if orig_url != None:
        return orig_url
    result = col.find({"tiny": {"$eq": tinyurl}})
    count = result.count()
    if count == 0:
        return None
    orig_url = None
    # this would loop for only one iteraton since there is
    # only single mapping corresponding to the given tinyurl.
    for doc in result:
        orig_url = doc["orig"]
    return orig_url

def get_algorithm():
    result = col.find({"url_algo": {"$exists": 1}})
    if result.count() > 0:
        # algorithm is already created.
        # Discard any environment variable for
        # unmatching algorithm
        algo = result[0]["url_algo"]
        return algo
    try:
        algo = os.environ["url_algo"]
    except KeyError:
        algo = "hash" 
    if algo not in ["sequential", "hash"]:
        print("Invalid algorithm name specified!")
        return None
    try:
        col.insert_one({"url_algo": algo})
    except Exception as e:
        print(str(e))
        return None
    return algo