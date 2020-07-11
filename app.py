import falcon
from threading import Thread

from routes.Ping import Ping
from routes.Subscribers import Subscribers
from processes.main_processor import heart_beat, default_consumer
from workers.worker_pool import DEFAULT_WORKER_THREAD

api = falcon.API()
api.add_route('/ping', Ping())
api.add_route('/api/v1/topics/message', Subscribers())

t = Thread(target=heart_beat, args=[default_consumer], daemon=True)
t.start()
