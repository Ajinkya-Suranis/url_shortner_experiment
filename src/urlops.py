import random
from char_mapping import url_char_mapping
from permutations import gen_next_permutation

# 26 lower + 26 upper + 10 numbers = 62 letters
TOTAL_CHARS = 62
STR_LEN = 6

# Current, updated set of letters.
current_letters = []
orig_letters = []

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
def create_tinyurl_letters(col):
    global current_letters, orig_letters
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
def get_tinyurl_string(redis_cli, url, col):
    global current_letters, orig_letters, TOTAL_CHARS, STR_LEN
    prev_letters = current_letters[:]
    current_letters = gen_next_permutation(prev_letters, orig_letters, TOTAL_CHARS, STR_LEN)
    col.update_one({"current_letters": prev_letters}, {"$set": {"current_letters": current_letters}})
    url_string = ''
    for i in current_letters:
        url_string += url_char_mapping[i]
    redis_cli.write_url_pair(url, url_string)
    col.insert_one({"orig": url, "tiny": url_string})
    return url_string