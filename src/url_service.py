import pymongo
from flask import Flask, request, render_template, redirect
from urlops import get_tinyurl_string, create_tinyurl_letters

app = Flask(__name__)
cli = pymongo.MongoClient("mongodb://localhost:27017/")
db = cli["urldb"]
col = db["urlcol"]

@app.route('/')
def entry():
    return render_template('makeittiny.html')

@app.route('/makeittiny', methods=['POST'])
def make_tiny():
    url = request.form['url']
    print("The URL is " + url)
    return makeittiny(url)

@app.route('/<tinyurl>')
def redirect_to_url(tinyurl):
    ret = search_tinyurl(tinyurl)
    if ret == None:
        return "Error 404: The tiny URL %s not found", 404
    return redirect(ret, code=302)

# Search if the tiny URL is already present
# in mongodb. Return the original URL if
# exists, None if not.
def search_tinyurl(tinyurl):
    global col
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

# Search if the original url(long one) already exists
# in mongodb. If so, return corresponding tiny url,
# else return None.

def search_origurl(origurl):
    global col
    result = col.find_one({"orig": {"$eq": origurl}})
    if result == None:
        return None
    # Since we've used find_one() above for querying,
    # there is only one item in the result.
    return result["tiny"]


# Add a new entry
def create_tinyurl(url):
    global col
    tinyurl_string = get_tinyurl_string(url, col)
    return tinyurl_string

def makeittiny(url):
    ret = search_origurl(url)
    if ret != None:
        print("URL found!")
        return "http://localhost:5000/" + ret
    return "http://localhost:5000/" + create_tinyurl(url)

if __name__ == '__main__':
    create_tinyurl_letters(col)
    app.run(host='0.0.0.0')