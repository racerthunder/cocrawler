#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
from functools import wraps
import logging
from tqdm import tqdm

import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL
from cocrawler.proxy import proxy_checker


from _BIN.proxy import Proxy

path = pathlib.Path(__file__).resolve().parent.parent / 'data' / 'top-1k.txt'

TOTAL = sum([1 for x in path.open()])


LOGGER = logging.getLogger(__name__)

proxy = Proxy()
PROXY_TOKEN = 'data2'




def dispatcher():

    urls = [line.strip() for line in path.open()]


    for url in urls:
        yield 'http://{0}'.format(url)

        #break

class Cruzer(cocrawler.Crawler):


    def task_generator(self):
        counter = 0
        dis = dispatcher()


        for url in tqdm(dis,total=TOTAL):

            counter +=1
            url = 'https://httpbin.org/post'
            proxy_url = proxy.get_next_proxy_cycle(url)
            req = Req(proxy_url,source_url=url)
            domain = req.url.hostname_without_www

            cookie = {'data':domain,'data2':'val2'}
            req.post = cookie


            yield Task(name='download',req=req,counter=counter,domain=domain)


            if counter > 0:
                break

    @proxy_checker(proxy,PROXY_TOKEN)
    def task_download(self,task):

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            print(task.doc.html)

        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))


    def task_second(self,task):
        if task.doc.status  == 200:
            print('good222: {0} , last_url: {1}'.format(task.req.url.hostname,task.last_url))
        else:
            print('--> bad code in second: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass




def misc():
    p = Proxy()
    print(p.get_next_proxy_cycle('http://tut.by'))

if __name__ == '__main__':
    '''
    command line args example: --config Fetcher.Nameservers:8.8.8.8 --loglevel INFO --reuse_session
    '''
    Cruzer.run()

    #misc()





