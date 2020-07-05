class CustomException(Exception):
    def __init__(self, status_code, message, info=dict()):
        self.status_code = status_code
        self.message = message
        self.info = info

    def __str__(self):
        return self.message
