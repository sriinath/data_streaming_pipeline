import falcon

class Base:
    def on_get(self, request, response):
        response.status = falcon.HTTP_200
        response.body = 'THE NORTH REMEMBERS...'
