
"""
Acknowledgments: 
1. https://alpscode.com/blog/how-to-use-reddit-api/
2. https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c
"""


from pprint import pprint
import os

import requests


CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]

ACC_BASE_URL = 'https://www.reddit.com/' 
ACC_TOK_ENDPOINT = "api/v1/access_token"

BASE_URL = "https://oauth.reddit.com"

AUTH = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
DATA = {"grant_type": "password", "username": USERNAME, "password": PASSWORD}
HEADERS = {"User-Agent": "data_for_nlp1/0.0.1"}

# ENDPOINT = "/r/changemyview/comments/l3ctyx/cmv_claiming_that_working_hard_or_making_changes/"
ENDPOINT = "/r/changemyview/top"


res = requests.post(ACC_BASE_URL + ACC_TOK_ENDPOINT,
                    data=DATA, headers=HEADERS, auth=AUTH)

tok = "Bearer " + res.json()["access_token"]
HEADERS["Authorization"] = tok

response = requests.get(BASE_URL + ENDPOINT, headers=HEADERS)

pprint(response.json())



