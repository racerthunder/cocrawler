from .urls import URL
from .document import Document
from .cookie import CookieManager

class Req():
    '''
    request class holding method and data for post
    '''
    def __init__(self,url,post=None):
        assert url is not None
        self.source_url = url
        self.url = URL(self.source_url)
        self.post = post
        self.cookies = CookieManager()

    @property
    def method(self):
        if self.post is not None:
            return 'POST'
        else:
            return 'GET'

    def set_post(self,data):
        if not isinstance(data,dict):
            raise ValueError('--> post must be Dict')
        self.post = data

    def set_cookie(self,data):
        if not isinstance(data,dict):
            raise ValueError('--> cookie must be Dict')
        self.cookies = data

class Task():
    def __init__(self,name,req,raw=False,**kwargs):
        self.req = req
        self.last_url = None # store final address of the request
        self.name = name
        self.doc = Document()
        self.raw = raw # if true make callback to function with any code, if False only 200 code gets passed down the road


        if kwargs:
            for k,v in kwargs.items():
                setattr(self,k,v)


