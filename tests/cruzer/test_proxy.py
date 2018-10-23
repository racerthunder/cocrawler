from functools import wraps
from types import MethodType
from collections import namedtuple
import traceback
import inspect
from _BIN.proxy import Proxy
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.document import Document
import operator

def task_generator_wrapper(method):

    #@wraps(method)
    def inner(self):
        print('--- innerrr',inner)
        _task = get_new_task()
        _task.name = 'wrapper'
        yield _task

        yield StopIteration()


    return inner

def get_new_task():
    return namedtuple('task','name counter')

class ProxyToken():
    def __init__(self,token):
        self.token = token

proxy = Proxy()
PROXY_TOKEN = ProxyToken('data2')

def proxy_checker(proxy,proxy_token):
    def proxy_inner(method):

        @wraps(method)
        def _impl(self,task):
            if task.counter == 0:
                print('--> bad proxy for task: {0}'.format(task.name))
                _task = get_new_task()
                _task.name = 'wrapper'
                _task.counter = task.counter + 1
                yield _task
                yield StopIteration()

            else:

                print('--> proxy is alive task: {0}'.format(task.name))

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


class Crawler():

    def __init__(self):
        self.url = 'google.com'

    def run(self):

        print('--> running cruzer run')

        funct = getattr(self,'task_download')
        _t = get_new_task()
        _t.name = 'crawler_task'
        _t.counter = 1 # 0 = make wrapper think this is the bad proxy


        try:
            _iter = iter(funct(_t))
        except TypeError:
            traceback.print_exc()
            print('--> No task left in Crawler')

        else:
            for _task in _iter:
                if isinstance(_task,StopIteration):
                    print('--> no task left')
                    break
                print('--> new task generated: {0}'.format(_task.name))


class CruzerProxy(Crawler):


    def __init__(self):
        super().__init__()
        proxys = [val for name,val in globals().items() if isinstance(val,Proxy)]
        if not len(proxys):
            raise ValueError('--> Proxy not defined!')
        proxy = proxys[0]

        proxy_tokens = [val for name,val in globals().items() if isinstance(val,Proxy)]
        if not len(proxy_tokens):
            raise ValueError('--> Proxy token not defined!')
        proxy_token = proxy_tokens[0]

        func_ls = [(name,val) for name,val in CruzerProxy.__subclasses__()[0].__dict__.items() if name.startswith('task_')]

        for name,class_func in func_ls:
            setattr(self, name, MethodType(proxy_checker(proxy,proxy_token)(class_func),self))




class Cruzer(CruzerProxy):

    #@proxy_checker
    def task_download(self,task):

        print('--> running download for task: {0}'.format(task.name))
        _t = get_new_task()
        _t.name = 'cruzer_download'
        yield _t




def main():
    cruzer = Cruzer()

    cruzer.run()



def test_checker():

    from cocrawler.proxy import ProxyChecker
    import operator
    import inspect


    class Mock():
        left = []
        right = None
        OP = None

        def __getattr__(self, item):
            print('--> [mock] : {0}'.format(item))
            self.__class__.left.append(item)
            return self

        def __eq__(self, other):
            print('--> [mock eq]: {0}'.format(other))
            self.__class__.OP = operator.eq
            self.__class__.right = other
            return False

        def __ne__(self, other):
            print('--> [mock not eq]: {0}'.format(other))
            self.__class__.OP = operator.ne
            self.__class__.right = other
            return False

        def __contains__(self, __token):
            print('--> [mock _contains]: {0}'.format(__token))
            self.__class__.OP = operator.contains

            if isinstance(__token, str):
                self.__class__.right = [__token,]
            else:
                self.__class__.right = list(__token)

            return False


    class TaskProxy():

        _cls_mock = Mock

        def __init__(self):
            self._cls_mock.left = []
            self._cls_mock.right = None
            self._cls_mock.OP = None

        def __getattr__(self, item):
            self._cls_mock.left.append(item)
            print('--> getattr: {0}'.format(item))
            return self._cls_mock()


        def get_cmd(self):
            return (self._cls_mock.left, self._cls_mock.right, self._cls_mock.OP)


    req1 = Req('http://google.com')
    t1 = Task(name='t1',req=req1)
    t1.doc.html = 'find me token'
    t1.doc.status = 200

    #
    # tproxy = TaskProxy()
    # res = (['token','mee'] < tproxy.doc.html)
    #
    # checker = ProxyChecker(*tproxy.get_cmd(), condition=any)


    tproxy2 = TaskProxy()
    res = (tproxy2.doc.status != 403)

    checker = ProxyChecker(*tproxy2.get_cmd(), condition=any)

    print(checker.validate(t1))

if __name__ == '__main__':
    #main()
    test_checker()

