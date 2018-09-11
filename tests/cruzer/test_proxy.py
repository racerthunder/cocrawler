from functools import wraps
from types import MethodType
from collections import namedtuple
import traceback
import inspect
from _BIN.proxy import Proxy

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
        _t.counter = 0 # 0 = make wrapper think this is the bad proxy


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
            setattr(self,name,MethodType(proxy_checker(proxy,proxy_token)(class_func),self))




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

if __name__ == '__main__':
    main()
