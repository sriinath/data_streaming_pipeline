import falcon
from threading import Thread

from routes.Ping import Ping
from routes.Topics import Topics

api = falcon.API()
api.add_route('/ping', Ping())
api.add_route('/api/v1/topics', Topics())
