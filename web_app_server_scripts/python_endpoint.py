import sys, requests
sys.path.insert(0,'.')
from functools import wraps
from flask import Flask, request, current_app, redirect, jsonify
from profile_dataset import *

app = Flask(__name__)


def support_jsonp(f):
    """Wraps JSONified output for JSONP"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            content = str(callback) + '(' + str(f().data) + ')'
            return current_app.response_class(content, mimetype='application/json')
        else:
            return f(*args, **kwargs)
    return decorated_function


@app.route('/process_dataset/<string:dataset>', methods=['GET'])
@support_jsonp
def get_dataset_info(dataset):
    json_return = get_data_stats(dataset)
    return jsonify(json_return)


@app.route('/submit_profile/<string:profile>', methods=['GET'])
@support_jsonp
def schedule_profile(profile):
    #json_return = get_data_stats(dataset)
    return "Successful endpoint"

    #res = requests.post("http://127.0.0.1:5000/determine_escalation/", json=s).json()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)