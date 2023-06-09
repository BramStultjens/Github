import json

from flask import Flask, redirect, session, request
from flask_cors import CORS, cross_origin
from auth.auth import authorize, oauth2callback
from customers.list_accessible_customers import list_accessible_customers
from ga_runner import REFRESH_ERROR

_CLIENT_URL = "http://localhost:4200"


app = Flask(__name__)
app.secret_key = "SECRET KEY"
cors = CORS(app, resources={r"/*": {"origins": _CLIENT_URL}})

@app.route("/authorize")
def authorize_endpoint():
    token = request.args.get("token")
    session["token"] = token
    auth_info = authorize()
    passthrough_val = auth_info["passthrough_val"]
    session["passthrough_val"] = passthrough_val
    url = auth_info["authorization_url"]
    return redirect(url)

@app.route("/oauth2callback")
def oauth2callback_endpoint():
    token = session["token"]
    passthrough_val = session["passthrough_val"]
    state = request.args.get("state")
    code = request.args.get("code")
    oauth2callback(passthrough_val, state, code, token)
    return redirect(_CLIENT_URL)

@app.route("/customers")
@cross_origin()
def customers():
    headers = request.headers
    token = headers["token"]
    try:
        resource_names = list_accessible_customers(token)
        return resource_names
    except Exception as ex:
        return handleException(ex)
    
def handleException(ex):
    error = str (ex)
    if error == REFRESH_ERROR:
        return json.dumps({
            "code": 401,
            "name": "INVALID_REFRESH_TOKEN",
            "description": error
        })
    else:
        return json.dumps({
            "code": 500,
            "name": "INTERNAL_SERVER_ERROR",
            "description": error
        })