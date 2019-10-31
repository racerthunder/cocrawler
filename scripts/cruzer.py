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


from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL
import cocrawler

from _BIN.tools.logs import Counter
from _BIN.cmd_request import CmdRequest

LOGGER = logging.getLogger(__name__)



class Dispatcher():

    def __init__(self):

        self.selector = ['xxxx','aaaa']

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

            url = 'http://divas.by/stili/contemp'

            req = Req(url)
            #req.set_referer('http://google.com')
            #cookie = {'data':domain,'data2':'val2'}
            #req.get = cookie


            yield Task(name='download',req=req, domain=domain)
            break


    async def task_download(self,task):
        c_type = task.doc.content_data[0] if task.doc.content_data else None

        if task.doc.status  == 200:
            print('good: {0} , code: {2} last_url: {1} c_type: {3}'.format(task.domain,task.last_url, task.doc.status, c_type))

            if 'контемпорари' in task.doc.html:
                print(task.doc.html)

        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            print('last_url: {0}'.format(task.last_url))
            #print(task.doc.fetcher.body_bytes)


if __name__ == '__main__':
    '''
    command line args example: 
    python3 cruzer.py\
    --config Crawl.MaxWorkers:5\
    --config Crawl.MaxTries:3\
    --config Crawl.PageTimeout:30\
    --config Crawl.AllowExternalRedir:False\
    --loglevel INFO\
    --config Fetcher.ReuseSession:True\
    --config Fetcher.CrawlPrivate:True\
    --config Fetcher.CrawlLocalhost:True\
    '''
    Cruzer.run()

    #misc()





