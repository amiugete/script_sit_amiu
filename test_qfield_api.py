import requests
from credenziali import * 
from qfieldcloud_sdk import sdk

client = sdk.Client(
    url=qfield_url,
    username=qfield_user,
    password=qfield_pwd,
)

try:
    projects = client.list_projects()
except requests.exceptions.RequestException:
    print("Oops!")