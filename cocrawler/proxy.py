from functools import wraps
from types import MethodType
import logging
import inspect
import traceback

import asyncio

from .urls import URL
from . import Crawler

from _BIN.proxy import Proxy


_logger = logging.getLogger(__name__)

class ProxyToken():
    '''
    receives str or list for token
    condition: any,all
    '''
    def __init__(self,token,*,condition=any):
        self.condition = condition

        self.__token = token

        if isinstance(self.__token,str):
            self.tokens = [self.__token,]
        else:
            self.tokens = list(self.__token)





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
        async def _impl(self,task):
            LOGGER = logger or _logger

            if task.doc.html is None or not proxy_token.condition([token for token in proxy_token.tokens if token in task.doc.html]):
                LOGGER.debug('--> Bad proxy for task: {0}'.format(task.name))
                new_url = proxy.get_next_proxy_cycle(task.req.source_url)
                task_clone = task.clone_task()
                task_clone.req.url = URL(new_url)
                yield task_clone
                yield StopIteration()

            else:
                LOGGER.debug('--> proxy is alive task: {0}'.format(task.name))

                try:
                    if asyncio.iscoroutinefunction(method):
                        # no new task will be yielded, run function and return
                        f = asyncio.ensure_future(method(self, task),loop=self.loop)
                        # result is not needed here, just wait for completion
                        await f
                        yield StopIteration()

                    elif inspect.isasyncgenfunction(method):
                        # we have a generator, load all tasks to the queue
                        async for task in method(self, task):
                            yield task

                        yield StopIteration()

                    else:
                        raise ValueError('--> {0} is not a coroutine or asyncgenerator, instead = {1} \n\n'.format(method,type(method)))
                except Exception as ex:
                    traceback.print_exc()


        return _impl

    return proxy_inner


class CruzerProxy(Crawler):
    '''
    parse task_* method from class and reattach it to cruzer instance alrady decorated by proxy_wrapper
    '''

    def __init__(self):
        super().__init__()
        cruzer_vars = CruzerProxy.__subclasses__()[0].__dict__

        # --------> get proxy <--------#
        proxys = [val for name,val in cruzer_vars.items() if isinstance(val,Proxy)]
        if not len(proxys):
            raise ValueError('--> Proxy not defined! Add Proxy instance as class attribute')
        proxy = proxys[0]

        # ------> get proxy token <------ #
        proxy_tokens = [val for name,val in cruzer_vars.items() if isinstance(val,ProxyToken)]
        if not len(proxy_tokens):
            raise ValueError('--> Proxy token not defined! Add ProxyToken instance as class attribute')
        proxy_token = proxy_tokens[0]

        # ------> decorate task_* <------#
        func_ls = [(name,val) for name,val in cruzer_vars.items() if name.startswith('task_')]
        if not len(func_ls):
            raise ValueError('--> Cruzer class mush have at least one "task_*" ')

        for name,class_func in func_ls:
            setattr(self, name, MethodType(proxy_checker(proxy,proxy_token)(class_func), self))


