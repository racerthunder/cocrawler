from cocrawler.req import Req
from cocrawler.task import Task
from cocrawler.urls import  URL
from furl import furl

def misc():

    req = Req('http://tut.by')
    print(req.url.url)


if __name__ == '__main__':
    #main()
    misc()

