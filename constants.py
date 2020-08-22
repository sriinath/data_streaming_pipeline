import os

BROKER_URLS = os.environ.get('BROKER_URL', ['localhost:9092'])
GROUP_ID = os.environ.get('group_id', 'default')
MAX_POLL_DELAY = os.environ.get('MAX_POLL_DELAY', 10)
MIN_POLL_DELAY = os.environ.get('MIN_POLL_DELAY', 0.5)
HEARTBEAT_INTERVAL = os.environ.get('HEARTBEAT_INTERVAL', 5)

MAX_RECORDS_PER_POLL = os.environ.get('MAX_RECORDS_PER_POLL', 200)
MAX_MESSAGES_PER_CONSUMER = os.environ.get('MAX_MESSAGES_PER_CONSUMER', 2000)
DEFAULT_CONSUMERS = os.environ.get('DEFAULT_CONSUMERS', 1)
MAX_CONSUMERS = os.cpu_count() * 5

INTERNAL_BROKER_URL = os.environ.get('INTERNAL_BROKER_URL', BROKER_URLS)
INTERNAL_PARTITIONS = os.environ.get('INTERNAL_PARTITIONS')
INTERNAL_GROUP_ID = os.environ.get('INTERNAL_GROUP_ID', 'internal')
