#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from tqdm import tqdm

import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req

import peewee

from _BIN.proxy import Proxy
from _DB.schemes.secondhand import diff_2018_feb_mar_apr,database


TOTAL = 1


LOGGER = logging.getLogger(__name__)



def dispatcher():

    urls = ['']


    for url in urls:
        yield 'http://{0}'.format(url)

        #break


# class Cruzer(CruzerProxy):
#     proxy = Proxy()
#     PROXY_TOKEN = ProxyToken(['data2'],condition=any)

class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        counter = 0
        dis = dispatcher()
        self.datalayer.peewee = database

        for url in tqdm(dis,total=TOTAL):

            counter +=1
            url = 'https://httpbin.org/get'
            #url = 'https://google.com'

            req = Req(url)
            domain = req.url.hostname_without_www

            params = {'data':domain,'data2':'val2'}
            req.get = params


            yield Task(name='download',req=req,counter=counter,domain=domain)


            if counter > 0:
                break

    async def task_download(self,task):

        try:
            selector = await self.datalayer.peewee.get(diff_2018_feb_mar_apr,diff_2018_feb_mar_apr.id==700002344534)
            print('sleeeet: ',selector)

        except peewee.DoesNotExist:
            print('--> not recrods found')


        # selector = await self.datalayer.peewee.execute(diff_2018_feb_mar_apr.select().where(diff_2018_feb_mar_apr.domain=='esnips.com'))
        # print('counnttt: ',selector)

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            print(task.doc.html)

        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
        #
        # url = 'https://httpbin.org/get'
        #
        # req = Req(url)
        # yield Task(name='second',req=req)

    async def task_second(self,task):
        selector = await self.datalayer.peewee.execute(diff_2018_feb_mar_apr.select().where(diff_2018_feb_mar_apr.domain=='esnips.com'))
        print('counnttt: ',selector)



def misc():
    p = Proxy()
    print(p.get_next_proxy_cycle('http://tut.by'))

if __name__ == '__main__':
    '''
    command line args example: --config Fetcher.Nameservers:8.8.8.8 --loglevel INFO --reuse_session
    '''
    Cruzer.run()

    #misc()





