import falcon

from routes.Ping import Ping
from routes.Subscribers import Subscribers
from processes.kafka_manager import run_processes

api = falcon.API()
api.add_route('/ping', Ping())
api.add_route('/api/v1/topics/message', Subscribers())

run_processes()
