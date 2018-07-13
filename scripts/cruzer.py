#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib


import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.counter import CounterBar
import time

counter = CounterBar()

def dispatcher():

    path = pathlib.Path(__file__).parent.parent / 'data' / 'top-1k.txt'
    urls = [line.strip() for line in path.open()]

    counter.init(len(urls))

    for url in urls:
        yield 'http://{0}'.format(url)
        #break

class Cruzer(cocrawler.Crawler):


    def task_generator(self):
        counter = 0
        for url in dispatcher():
            counter +=1
            url = 'https://httpbin.org/forms/post'

            req = Req(url)
            domain = req.url.hostname_without_www
            #cookie = {'data':domain,'data2':'val2'}

            #req.set_cookie(cookie)
            yield Task(name='download',req=req,counter=counter,domain=domain)

            if counter > 0:
                break

    def task_download(self,task):

        counter.count()

        if task.doc.status  == 200:

            task.doc.set_input('custname','valvalaval')
            req = task.doc.get_req()
            #print(vars(req))
            yield Task(name='second',req=req)

        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass


    def task_second(self,task):
        if task.doc.status  == 200:
            print(task.doc.html)
        else:
            print('--> bad code in second: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: --config Crawl.MaxWorkers:15 --loglevel INFO --reuse_session
    '''
    Cruzer.run()
