import sys, requests, json
sys.path.insert(0,'.')
from functools import wraps
from flask import Flask, request, current_app, redirect, jsonify
from profile_dataset import *
from profile import *

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
    ec2_conn = start_ec2_boto_connection()
    s3_conn = start_s3_boto_connection()
    profile_json = json.loads(profile)
    reservation, save_dir, actual_bid_price = euca_spot_launch_mimicry(ec2_conn, profile_json['bidPerMachine'], profile_json['machineType'], profile_json['numberOfMachines'])
    ret_dict = {}
    ret_dict['reservation'] = reservation
    ret_dict['save_dir'] = save_dir
    ret_dict['actual_bid_price'] = actual_bid_price

    return jsonify(ret_dict)

    #res = requests.post("http://127.0.0.1:5000/determine_escalation/", json=s).json()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)