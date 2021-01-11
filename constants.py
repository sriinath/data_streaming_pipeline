import os
import json

BROKER_URLS = os.environ.get('BROKER_URL', 'localhost:9092').split(',')
GROUP_ID = os.environ.get('GROUP_ID', 'default')
CONSUMER_TOPICS = os.environ.get('CONSUMER_TOPICS', '').split(',')
MAX_POLL_DELAY = os.environ.get('MAX_POLL_DELAY', 10)
MIN_POLL_DELAY = os.environ.get('MIN_POLL_DELAY', 0.5)
HEARTBEAT_INTERVAL = os.environ.get('HEARTBEAT_INTERVAL', 5)

MAX_RECORDS_PER_POLL = os.environ.get('MAX_RECORDS_PER_POLL', 200)
MAX_MESSAGES_PER_CONSUMER = os.environ.get('MAX_MESSAGES_PER_CONSUMER', 2000)
MAX_CONSUMER_PROCESS = os.environ.get('MAX_CONSUMER_PROCESS', 2)
MAX_WORKER_THREADS = os.environ.get('MAX_WORKER_THREADS', 5)

CALLBACK_URL = os.environ.get('CALLBACK_URL')
CALLBACK_HEADERS = json.loads(os.environ.get('CALLBACK_HEADERS', '{}'))
SUBSCRIBER_INFO_URL = json.loads('SUBSCRIBER_INFO_URL')
SUBSCRIBER_INFO_HEADERS = json.loads(os.environ.get('SUBSCRIBER_INFO_HEADERS', '{}'))

HEALTHCHECK_HASHKEY = os.environ.get('HEALTHCHECK_HASHKEY', 'health_check')
META_INFO_HASHKEY = os.environ.get('META_INFO_HASHKEY', 'consumer:meta')
