import hashlib
import os
from .secret import Secret
import logging

from google_auth_oauthlib.flow import Flow

_CLIENT_SECRETS_PATH = os.environ["CLIENT_SECRETS_PATH"]
_SCOPE = "https://www.googleapis.com/auth/adwords"
_SERVER = "127.0.0.1"
_PORT = 5000
_REDIRECT_URI = f"http://{_SERVER}:{_PORT}/oauth2callback"
_REFRESH_TOKEN = "1//xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

def authorize():
    flow = Flow.from_client_secrets_file(_CLIENT_SECRETS_PATH, scopes=[_SCOPE])
    flow.redirect_uri = _REDIRECT_URI

    passthrough_val = hashlib.sha256(os.urandom(1024)).hexdigest()

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        state=passthrough_val,
        prompt="consent",
        include_granted_scopes="true"
    )

    return { "authorization_url": authorization_url, "passthrough_val": passthrough_val }

def oauth2callback(passthrough_val, state, code, token):
    if passthrough_val != state:
        message = "State token does not match the expected state."
        raise ValueError(message)
        
    flow = Flow.from_client_secrets_file(_CLIENT_SECRETS_PATH, scopes=[_SCOPE])
    flow.redirect_uri = _REDIRECT_URI
    flow.fetch_token(code=code)
    refresh_token = flow.credentials.refresh_token
    refresh_token = _REFRESH_TOKEN
    logging.basicConfig(filename='refresh_token5.log', encoding='utf-8', level=logging.INFO)
    logging.info("Refresh token: %s", refresh_token)
    secret = Secret(token)
    secret.create_secret_version(refresh_token)