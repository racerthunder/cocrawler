#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from subprocess import check_output
import time
import asyncio
from yarl import URL as yURL

import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL

from _BIN.tools.logs import Counter

LOGGER = logging.getLogger(__name__)



class Dispatcher():

    def __init__(self):

        self.selector = [line.strip() for line in open('list.txt')]

        self.sel_iter = iter(self.selector)
        self.total = self.get_total()

    def __iter__(self):
        return self

    def get_total(self):
        return 99
        total = self.selector.count()
        print('--> records selected: {0}'.format(total))
        return total

    def __next__(self):
        row = next(self.sel_iter)
        return (row)



class Cruzer(cocrawler.Crawler):

    def task_generator(self):

        dis = Dispatcher()
        counter = Counter(dis.total)

        for domain in dis:

            counter.count()


            ctype = 2

            url = f'https://majestic.com/charts/referring-domains-discovery-chart/?d={domain}&w=1000&h=250&t=l&a=0&ctype={ctype}&bc=EAEEF0&IndexDataSource=H&entries=100'

            req = Req(url)
            #domain = req.url.hostname_without_www

            #cookie = {'data':domain,'data2':'val2'}
            #req.get = cookie


            yield Task(name='download',req=req,domain=domain)
            #break


    async def task_download(self,task):

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            save_path = '/Volumes/crypt/_Coding/PYTHON/_FILES/images/{0}.png'.format(task.domain)
            task.doc.save(save_path)

        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: 
    python3 cruzer.py\
    --config Crawl.MaxWorkers:5\
    --config Crawl.MaxTries:3\
    --config Crawl.DumpMemory:True\
    --config Crawl.AllowExternalRedir:False\
    --loglevel INFO\
    --reuse_session
    '''
    Cruzer.run()

    #misc()





