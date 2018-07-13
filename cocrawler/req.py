import aiohttp

from .urls import URL


class Req():
    '''
    request class holding method and data for post
    '''
    def __init__(self,url,post=None):
        self.source_url = url
        self.url = URL(self.source_url)
        self.post = post
        self.cookies = None
        self.multipart_post = False

    def default_config(self):
        return dict(
            post = None,
            cookies = None,
            multipart_post = False,
        )
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

    def set_multipart(self,val):
        if not isinstance(val,bool):
            raise ValueError('--> multipart must be True or False')
        self.multipart_post = val

    def set_url(self,url):
        self.url = URL(url)

    def reset(self):
        self.cookies = None
        self.post = None
        self.multipart_post = False

