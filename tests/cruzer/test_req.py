from cocrawler.req import Req
from cocrawler.task import Task
from cocrawler.urls import  URL


def misc():

    req = Req('http://tut.by')
    task1 = Task(name='task1',req=req)

    task2 = task1.clone_task()
    task2.req.url = URL('http://google.com')

    print(task2.req)

if __name__ == '__main__':
    #main()
    misc()
