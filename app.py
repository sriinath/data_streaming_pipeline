import falcon

from routes.Base import Base

api = falcon.API()
api.add_route('/ping', Base())

if __name__ == "__main__":
    from wsgiref import simple_server
    httpd = simple_server.make_server('127.0.0.1', 8000, api)
    httpd.serve_forever()
