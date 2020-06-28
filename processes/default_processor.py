from workers.worker_pool import DEFAULT_WORKER_THREAD
from workers.kafka_worker.kafka_manager import default_consumer, default_producer
from constants import INTERNAL_CONSUMER_TOPIC

def default_processor():
    print('Starting the processor in worker for consuming default messages')
    DEFAULT_WORKER_THREAD.process_tasks(
        default_consumer.consume_messages, processor
    )
    print('Successfully added the task for consuming default message')

def processor(message):
    print(message)
    default_producer.send_message(INTERNAL_CONSUMER_TOPIC, message)

