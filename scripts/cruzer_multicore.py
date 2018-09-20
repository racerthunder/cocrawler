#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from tqdm import tqdm
from subprocess import check_output

import asyncio


import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL
#from cocrawler.proxy import CruzerProxy,ProxyToken



path = pathlib.Path(__file__).resolve().parent.parent / 'data' / 'top-1k.txt'


def get_lines_count(path):

    res = check_output(['wc','-l',str(path)])
    count = int(res.split()[0])

    return count



LOGGER = logging.getLogger(__name__)



def dispatcher():

    urls = [line.strip() for line in path.open()]


    for url in urls:
        yield 'http://{0}'.format(url)

        #break


#from _BIN.proxy import Proxy
# class Cruzer(CruzerProxy):
#     proxy = Proxy()
#     PROXY_TOKEN = ProxyToken(['data2','User-Agent'],condition=any)

class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        counter = 0
        dis = dispatcher()
        total = get_lines_count(path)

        for url in tqdm(dis,total=total):

            counter +=1
            #url = 'http://httpbin.org/get'

            #proxy_url = self.proxy.get_next_proxy_cycle(url)
            req = Req(url,source_url=url)
            domain = req.url.hostname_without_www

            cookie = {'data':domain,'data2':'val2'}
            req.get = cookie


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


if __name__ == '__main__':
    '''
    command line args example: --config Crawl.MaxWorkers:300 --loglevel INFO --reuse_session
    '''
    Cruzer.run()

    #misc()





