import os

QUEUE_URL = os.getenv('QUEUE_URL')
QUEUE_PORT = os.getenv('QUEUE_PORT')
QUEUE_USERNAME = os.getenv('QUEUE_USERNAME')
QUEUE_PASSWORD = os.getenv('QUEUE_PASSWORD')
QUEUE_NAME = os.getenv('QUEUE_NAME')
QUEUE_VHOST = os.getenv('QUEUE_VHOST')


CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TOKEN_ENDPOINT = os.getenv('ENDPOINT')

# Elastic Search URL
ES_URL = os.getenv('SEARCHENGINE_URL')
