from functools import wraps
from types import MethodType
import logging

from .urls import URL
from . import Crawler

from _BIN.proxy import Proxy


_logger = logging.getLogger(__name__)

class ProxyToken():
    def __init__(self,token):
        self.token = token


def proxy_checker(proxy,proxy_token,logger=None):
    '''
    Since body of the function contains 'yield' outside work sees is as generator, therefore every place where
    other should see as job completed should yield StopIteration()

    :param proxy: Proxy isntance for rotating proxy
    :param proxy_token: <str> token to find in thml response
    :param logger: <logging> instance of main crawler
    :return:
    '''
    def proxy_inner(method):
        @wraps(method)
        def _impl(self,task):
            LOGGER = logger or _logger
            if task.doc.html is None or (proxy_token not in task.doc.html):
                LOGGER.debug('--> Bad proxy for task: {0}'.format(task.name))
                new_url = proxy.get_next_proxy_cycle(task.req.source_url)
                task_clone = task.clone_task()
                task_clone.req.url = URL(new_url)
                yield task_clone
                yield StopIteration()

            else:
                LOGGER.debug('--> proxy is alive task: {0}'.format(task.name))

                result = method(self, task)

                try:
                    _it = iter(result)

                except TypeError:
                    yield StopIteration()

                else:
                    for _t in _it:
                        yield _t
                    else:
                        # make run function of Crawler think we are done with the job
                        yield StopIteration()


        return _impl

    return proxy_inner



class CruzerProxy(Crawler):
    '''
    parse task_* method from class and reattach it to cruzer instance alrady decorated by proxy_wrapper
    '''

    def __init__(self):
        super().__init__()
        cruzer_vars = CruzerProxy.__subclasses__()[0].__dict__.items()

        proxys = [val for name,val in cruzer_vars if isinstance(val,Proxy)]
        if not len(proxys):
            raise ValueError('--> Proxy not defined! Add Proxy instance as class attribute')
        proxy = proxys[0]

        proxy_tokens = [val for name,val in cruzer_vars if isinstance(val,ProxyToken)]
        if not len(proxy_tokens):
            raise ValueError('--> Proxy token not defined! Add ProxyToken instance as class attribute')
        proxy_token = proxy_tokens[0].token

        func_ls = [(name,val) for name,val in cruzer_vars if name.startswith('task_')]

        for name,class_func in func_ls:
            setattr(self,name,MethodType(proxy_checker(proxy,proxy_token)(class_func),self))

