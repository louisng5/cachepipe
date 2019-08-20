class CallProxy():
    def __init__(self,callcontext):
        self.callcontext = callcontext

    def __call__(self):
        return self.get_source()

    def tablename(self):
        return self.callcontext.adapter.tablename()

    def get_source(self):
        return self.callcontext.get_source()

    def cache_source(self):
        return self.callcontext.cache_source()

    def check_source(self):
        return self.callcontext.check_source()