#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from tqdm import tqdm

import asyncio

import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL
from cocrawler.proxy import CruzerProxy,ProxyToken

from _BIN.proxy import Proxy

path = pathlib.Path(__file__).resolve().parent.parent / 'data' / 'top-1k.txt'

TOTAL = sum([1 for x in path.open()])


LOGGER = logging.getLogger(__name__)



def dispatcher():

    urls = [line.strip() for line in path.open()]


    for url in urls:
        yield 'http://{0}'.format(url)

        #break


# class Cruzer(CruzerProxy):
#     proxy = Proxy()
#     PROXY_TOKEN = ProxyToken(['data2','User-Agent'],condition=any)

class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        counter = 0
        dis = dispatcher()


        for url in tqdm(dis,total=TOTAL):

            counter +=1
            url = 'https://httpbin.org/post'
            #proxy_url = self.proxy.get_next_proxy_cycle(url)
            req = Req(url,source_url=url)
            domain = req.url.hostname_without_www

            cookie = {'data':domain,'data2':'val2'}
            req.post = cookie


            yield Task(name='download',req=req,counter=counter,domain=domain)


            if counter > 0:
                break

    async def task_download(self,task):

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            print(task.doc.html)


        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))


    async def task_second(self,task):
        print('---herere')
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





