import sys
from redisops import url_class
from flask import Flask, request, render_template, redirect
import urlops

app = Flask(__name__)
redis_cli = None
algo = None

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
    global redis_cli
    print(tinyurl)
    ret = urlops.search_tinyurl(redis_cli, tinyurl)
    if ret == None:
        return "Error 404: The tiny URL %s not found", 404
    return redirect(ret, code=302)


# Add a new entry
def create_tinyurl(algo, url):
    global redis_cli
    tinyurl_string = urlops.get_tinyurl_string(algo, redis_cli, url)
    return tinyurl_string


def makeittiny(url):
    global redis_cli, algo
    ret = urlops.search_origurl(algo, redis_cli, url)
    if ret != None:
        print("URL found!")
        return "http://localhost:5000/" + ret
    return "http://localhost:5000/" + create_tinyurl(algo, url)


if __name__ == '__main__':
    try:
        redis_cli = url_class()
    except Exception as e:
        print(str(e))
        sys.exit()
    algo = urlops.get_algorithm()
    if algo == None:
        print("Failed to decide the algorithm. Exiting...")
        sys.exit()
    try:
        urlops.create_cron_daemon(algo)
    except Exception as e:
        print(str(e))
        sys.exit()
    if algo == "sequential":
        urlops.create_tinyurl_letters()
    app.run(host='0.0.0.0')