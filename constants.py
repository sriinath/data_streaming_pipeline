import os

MAX_POLL_DELAY = 10
MIN_POLL_DELAY = 0.5
HEARTBEAT_INTERVAL = 5

MAX_RECORDS_PER_POLL = 200
MAX_MESSAGES_PER_CONSUMER = 2000
MAX_CONSUMERS = os.cpu_count() * 5
DEFAULT_CONSUMERS = 1
