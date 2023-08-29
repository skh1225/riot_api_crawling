import requests
import time

from config import config

token = config.api_keys[0]
headers = config.headers(token)
schedule_time = time()