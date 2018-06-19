from .urls import URL
from .document import Document

class Task(object):
    def __init__(self,name,url=None,etree_required=False,raw=False,**kwargs):
        assert url is not None
        self.url = URL(url)
        self.name = name
        self.doc = Document()
        self.raw = raw # if true make callback to function with any code, if False only 200 code gets passed down the road


        if kwargs:
            for k,v in kwargs.items():
                setattr(self,k,v)
