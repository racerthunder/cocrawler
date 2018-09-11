from functools import wraps
from types import MethodType
from collections import namedtuple
import traceback
import inspect

def get_new_task():
    return namedtuple('task','name counter')

def proxy_checker(proxy_obj,prox):
    def proxy_wrapper(method):
        print(proxy_obj,prox)
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

    return proxy_wrapper

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
        print(self.task_download)
        self.task_download = MethodType(proxy_wrapper(self.task_download),self)
        print(self.task_download)
        super().__init__()

        # tasks = [name for name in dir(self) if name.startswith('task_')]
        #
        # objs = []
        #
        # for task in tasks:
        #
        #     print('--> before: ',getattr(self,task))
        #
        # for task in tasks:
        #     objs.append(getattr(self,task))
        #
        # bound_meth = MethodType(proxy_wrapper(objs[0]),self)
        #
        # setattr(self,'task_download', bound_meth)
        #
        # for task in tasks:
        #     print('--> after: ',getattr(self,task))




class Cruzer(Crawler):

    @proxy_checker('obbbjss','aaaaa')
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
