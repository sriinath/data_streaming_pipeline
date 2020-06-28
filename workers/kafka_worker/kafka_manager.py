import json

from .consumer import Consumer
from .producer import Producer
from constants import INTERNAL_CONSUMER_TOPIC 

default_producer = Producer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
default_consumer = Consumer()
internal_consumer = Consumer(INTERNAL_CONSUMER_TOPIC)
