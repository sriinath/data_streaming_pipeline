from workers.worker_pool import DEFAULT_WORKER_THREAD
from workers.kafka_worker.kafka_manager import internal_consumer, default_producer
from constants import INTERNAL_CONSUMER_TOPIC

def internal_processor():
    print('Starting the processor in worker for consuming internal messages')
    DEFAULT_WORKER_THREAD.process_tasks(
        internal_consumer.consume_messages, processor
    )
    print('Successfully added the task for consuming internal message')

def processor(message):
    print(message)
