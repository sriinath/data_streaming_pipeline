import falcon
from threading import Thread

from routes.Ping import Ping
from routes.Subscribers import Subscribers
from routes.Topics import Topics
from processes.auth import AuthMiddleware

api = falcon.API(middleware=[AuthMiddleware()])
api.add_route('/ping', Ping())
api.add_route('/api/v1/topics', Topics())
api.add_route('/api/v1/topics/subscribe', Subscribers())
