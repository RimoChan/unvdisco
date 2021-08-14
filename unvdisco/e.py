class HTTPError(Exception):
    def __init__(self, code, *li, **d):
        super().__init__(*li, **d)
        self.code = code
