from functools import wraps
from types import MethodType
import logging
import inspect
import traceback
import operator
from collections import defaultdict
from terminaltables import AsciiTable

import asyncio
from furl import furl

from .urls import URL
from . import Crawler

from _BIN.proxy import Proxy, NoAliveProxy


PROXY_CHECKERS = defaultdict(list)

_logger = logging.getLogger(__name__)

class ProxyCheckerError(Exception):pass
class BadProxySignal(ProxyCheckerError):pass

class ProxyCheckerBase():pass


class Mock():
    '''
    Mock's purpose is to record every attribute call to make a call chain
    '''
    left = []
    right = None
    OP = None

    def __getattr__(self, item):
        # return self here to make arbitary long enclosed attributes
        self.__class__.left.append(item)
        return self

    def __eq__(self, other):
        self.__class__.OP = operator.eq
        self.__class__.right = other
        return False

    def __ne__(self, other):
        self.__class__.OP = operator.ne
        self.__class__.right = other
        return False

    def __contains__(self, __token):
        '''

        :param __token: str or list
        :return:
        '''

        self.__class__.OP = operator.contains

        if isinstance(__token, str):
            self.__class__.right = [__token,]
        else:
            self.__class__.right = list(__token)

        return False

class TaskProxy():

    _cls_mock = Mock

    def __init__(self, need=True):
        '''
        every call to class properties is recorded and then used to construct calls chain and conseqeuntly get_cmd()
        '''
        self.need = need # could be used to invert decision, ex. _token in doc.html with False and not_contain , res= True
        self._cls_mock.left = []
        self._cls_mock.right = None
        self._cls_mock.OP = None

    def __getattr__(self, item):
        self._cls_mock.left.append(item)
        return self._cls_mock()

    def get_cmd(self):
        return (self._cls_mock.left, self._cls_mock.right, self._cls_mock.OP, self.need)


class ProxyChecker(ProxyCheckerBase):
    def __init__(self, left, right, OP, need=True, *, apply_for_task='all', condition=any):

        self.left = left
        self.right = right
        self.operator = OP
        self.need = need

        self.condition = condition
        self.apply_for_task = apply_for_task


    def pre_validate(self, task):

        for atr in self.left:
            # rewrite task object with every iteration
            task = getattr(task, atr, None)

        if self.operator == operator.contains:
            # here self.right is a list of tokens (at least one exists)
            # if there is any error in fetcher (ex 500, 403) html will always be None, therefore return False
            if task is None:
                return False
            is_valid = self.condition([True if token in task else False for token in self.right])
            return is_valid

        else:

            return self.operator(task, self.right)

    def validate(self, task):
        res = self.pre_validate(task)
        if res == self.need:
            return True
        else:
            return False


def proxy_checker_wrapp(proxy, proxy_checkers,logger=None):
    '''
    Since body of the function contains 'yield' outside world sees is as generator,
    therefore every place where other should see as job completed must yield StopIteration()

    :param proxy: Proxy isntance for rotating proxy
    :param proxy_checkers: <list><ProxyChecker><list> class for validating task_proxy agains real task
    :param logger: <logging> instance of main crawler
    :return:
    '''
    def generate_task_clone(task, self, logger):

        #proxy_bad = furl(task.init_proxy).remove(args=True,fragment_args=True).url
        proxy_bad = task.init_proxy
        proxy.mark_bad(proxy_bad)

        try:
            source_url = furl(task.req.url.url).args['q']
        except KeyError as ex:
            logger.warning('--> No proxy url found in link, its ok: {0}'.format(task.req.url.url))
            return StopIteration()

        new_proxy = self.new_proxy()
        if new_proxy is None:
            return StopIteration('--> No alive proxy left')

        new_proxy_url = (furl(new_proxy).add({'q':source_url})).url

        task_clone = task.clone_task()
        task_clone.req.url = URL(new_proxy_url)
        task_clone.init_proxy = new_proxy_url # if any redir within proxy dont loose original url to mark bad

        return task_clone

    def proxy_inner(method):
        @wraps(method)
        async def _impl(self,task):

            skip_check = False
            good_status = getattr(self, 'proxy_good_status', None)

            if good_status and task.doc.status is not None and task.doc.status in good_status:
                skip_check = True

            LOGGER = logger or _logger

            if not skip_check and not all([checker.validate(task) for checker in proxy_checkers]):

                LOGGER.debug('--> [Bad Proxy] for task: {0}, {1}'.format(task.name, task.req.url.url))

                task_clone = generate_task_clone(task, self, LOGGER)
                yield task_clone
                yield StopIteration()

            else:

                try:
                    if asyncio.iscoroutinefunction(method):
                        # no new task will be yielded, run function and return
                        f = asyncio.ensure_future(method(self, task),loop=self.loop)
                        _result = await f

                        # we can initiate proxy rotation directly from the callback function by returing BadProxySignal
                        if isinstance(_result, BadProxySignal):
                            LOGGER.debug('--> [Bad Proxy] *FROM* task: {0}, {1}'.format(task.name, task.req.url.url))
                            task_clone = generate_task_clone(task, self, LOGGER)
                            yield task_clone
                        else:
                            LOGGER.debug('--> [Alive proxy] task: {0}, host: {1}'.format(task.name,
                                                                                          task.req.url.hostname_without_www))

                        yield StopIteration()

                    elif inspect.isasyncgenfunction(method):
                        # we have a generator, load all tasks to the queue
                        LOGGER.debug('--> [Alive proxy] task: {0}, host: {1}'.format(task.name,
                                                                                      task.req.url.hostname_without_www))

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

    def get_proxy(self):
        # overload this method to cutom setup proxy isntance
        return Proxy(shuffle=True)

    def new_proxy(self):
        try:
            new_proxy = self.proxy.get_next_proxy_cycle()
            return new_proxy
        except NoAliveProxy as ex:
            traceback.print_exc()
            self.shutdown('--> No alive proxy left')
            return None

    def proxy_url(self, source_url):

        new_proxy = self.new_proxy()
        new_proxy_url = self.proxy.get_full_url(new_proxy, source_url)
        return new_proxy_url

    def __init__(self):

        super().__init__()
        self.proxy = self.get_proxy()

        cruzer_vars = CruzerProxy.__subclasses__()[0].__dict__

        # ------> get proxy token <------ #
        proxy_checkers = [val for name,val in cruzer_vars.items() if isinstance(val,ProxyCheckerBase)]
        if not len(proxy_checkers):
            raise ValueError('--> Proxy token not defined! Add ProxyToken instance as class attribute')

        # make list of task with corresponding checkers
        for _checker in proxy_checkers:
            if _checker.apply_for_task == 'all':
                PROXY_CHECKERS['all'].append(_checker)

            else:
                if isinstance(_checker.apply_for_task, str):
                    raise ValueError('--> "apply_for_task must" be list')

                for _task in _checker.apply_for_task:
                    PROXY_CHECKERS[_task].append(_checker)



        # ------> decorate task_* <------#
        func_ls = [(name,val) for name,val in cruzer_vars.items() if name.startswith('task_') and not
                   name=='task_generator']

        # find if any task_* exists
        if not len(func_ls):
            raise ValueError('--> Cruzer class mush have at least one "task_*" ')

        # check if all task_* are covered with at least one checker
        if 'all' not in PROXY_CHECKERS.keys():
            diff = set([x[0] for x in func_ls]).difference(set(PROXY_CHECKERS.keys()))

            if len(diff) > 0:
                raise ProxyCheckerError('--> Not all tasks are covered with checkers: {0}'.format(str(diff)))

        for name,class_func in func_ls:
            #iterate over all task_* functions

            _checkers = []
            if PROXY_CHECKERS.get(name, None):
                _checkers.extend(PROXY_CHECKERS.get(name))

            if 'all' in PROXY_CHECKERS:
                _checkers.extend(PROXY_CHECKERS['all'])


            _method = MethodType(proxy_checker_wrapp(self.proxy, _checkers)(class_func), self)

            setattr(self, name, _method)








