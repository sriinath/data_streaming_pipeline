import falcon
from routes.Ping import Ping
from routes.Subscribers import Subscribers

api = falcon.API()
api.add_route('/ping', Ping())
api.add_route('/api/v1/topics/message', Subscribers())

# if __name__ == "__main__":
#     from wsgiref import simple_server
#     httpd = simple_server.make_server('127.0.0.1', 8000, api)
#     httpd.serve_forever()
