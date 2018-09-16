import aiohttp


from .document import Document
import asyncio

class Task():
    def __init__(self,name,req,**kwargs):
        self.req = req
        self.last_url = None # store final address of the request
        self.name = name
        self.doc = Document(url=self.req.url.url)
        self.session_id=None # used with reuse_session==True
        self.cruzer=None # cruzer instance to get access to sessions pool
        self.flow = [] # names of the tasks that was executed before current one, if list is empty
                        # this task is considered to be a root which holds the first ref to session

        if kwargs:
            for k,v in kwargs.items():
                setattr(self,k,v)

    def add_parent(self,taks_name):
        self.flow.append(taks_name)

    def set_session_id(self,id):
        self.session_id = id

    def __str__(self):
        s = 'task: {0} , url: {1}'.format(self.name, self.req.url.url)
        return s


    def __await__(self):
        return self.worker().__await__()

    async def worker(self):
        return await asyncio.sleep(0.1)

    def cookie_list(self):
        '''
        simple view of cookie, just key and value pairs
        :return:
        '''
        if self.cruzer is None:
            raise ValueError('--> Cruzer instance is missing')
        else:
            session = self.cruzer.pool.get_session(self.session_id)

            if isinstance(session.cookie_jar,aiohttp.DummyCookieJar):
                raise ValueError('--> Trying to get cookie but cookiejar is dummy, enable --reuse_session for cruzer!')

            return dict([(cok._key,cok._value) for cok in session.cookie_jar])



    def clone_task(self):
        '''
        return same object but with cleared data
        :return:
        '''
        self.last_url = None
        self.doc = Document(url=self.req.url.url)
        self.session_id=None
        self.flow[:] = []
        self.cruzer=None

        return self
