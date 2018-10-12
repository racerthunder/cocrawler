from functools import wraps
from types import MethodType
import logging
import inspect
import traceback

import asyncio
from furl import furl

from .urls import URL
from . import Crawler

from _BIN.proxy import Proxy


_logger = logging.getLogger(__name__)

class ProxyCheckerError(Exception):pass


class ProxyCheckerBase():pass


class ProxyCheckerHTML(ProxyCheckerBase):
    '''
    receives str or list for token
    condition: any,all
    apply_for_task: default='all', applies for all task in the workflow, or individually
            ex. ['task_download',]
    '''
    def __init__(self,token,*, apply_for_task='all', condition=any):

        self.condition = condition

        self.apply_for_task = apply_for_task
        if self.apply_for_task != 'all' and not(isinstance(self.apply_for_task,list)):
            raise ValueError('--> apply_for_task must be list instead = {0}'.format(self.apply_for_task))

        self.__token = token

        if isinstance(self.__token, str):
            self.tokens = [self.__token,]
        else:
            self.tokens = list(self.__token)

    def validate(self,html):

        if html is None:
            return False

        is_valid = self.condition([token for token in self.tokens if token in html])
        return is_valid





def proxy_checker_wrapp(proxy,proxy_checker,logger=None):
    '''
    Since body of the function contains 'yield' outside world sees is as generator,
    therefore every place where other should see as job completed must yield StopIteration()

    :param proxy: Proxy isntance for rotating proxy
    :param proxy_checker: <ProxyChecker> token to find in thml response
    :param logger: <logging> instance of main crawler
    :return:
    '''
    def proxy_inner(method):
        @wraps(method)
        async def _impl(self,task):
            LOGGER = logger or _logger

            if not proxy_checker.validate(task.doc.html):

                LOGGER.debug('--> Bad proxy for task: {0}'.format(task.name))

                source_url = furl(task.req.url.url).args['q']
                new_proxy = proxy.get_next_proxy_cycle()
                new_proxy_url = (furl(new_proxy).add({'q':source_url})).url

                task_clone = task.clone_task()
                task_clone.req.url = URL(new_proxy_url)

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

class CollisionsList(list):
    '''
    do not allow to append duplicates
    '''
    def append(self, other):
        if other in self:
            raise ValueError('--> Value already added: {0}'.format(other))
        super().append(other)

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
        proxy_checkers = [val for name,val in cruzer_vars.items() if isinstance(val,ProxyCheckerBase)]
        if not len(proxy_checkers):
            raise ValueError('--> Proxy token not defined! Add ProxyToken instance as class attribute')


        # ------> decorate task_* <------#
        func_ls = [(name,val) for name,val in cruzer_vars.items() if name.startswith('task_') and not
                   name=='task_generator']

        if not len(func_ls):
            raise ValueError('--> Cruzer class mush have at least one "task_*" ')

        proxy_covered_funcs = CollisionsList() # list of functions that proxy checkers is applied
        for name,class_func in func_ls:

            for proxy_checker in proxy_checkers:
                _method = MethodType(proxy_checker_wrapp(proxy,proxy_checker)(class_func), self)

                if proxy_checker.apply_for_task == 'all':
                    proxy_covered_funcs.append(name)
                    setattr(self, name, _method)
                else:
                    for applied_task in proxy_checker.apply_for_task:
                        if applied_task == name:
                            proxy_covered_funcs.append(name)
                            setattr(self, name, _method)


        diff = set([x[0] for x in func_ls]).difference(set(proxy_covered_funcs))

        if len(diff) > 0:
            raise ProxyCheckerError('--> Not all tasks are covered with checkers: {0}'.format(str(diff)))




