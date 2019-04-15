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

            url = 'https://francemeds.com'
            #url = 'https://www.whatismybrowser.com/detect/what-http-headers-is-my-browser-sending'
            req = Req(url)

            yield Task(name='download',req=req,domain=domain)
            break


    async def task_download(self,task):
        c_type = task.doc.content_data[0] if task.doc.content_data else None

        if task.doc.status  == 200:
            print('good: {0} , code: {2} last_url: {1} c_type: {3}'.format(task.domain,task.last_url, task.doc.status, c_type))
            print('ip: ', task.host_ip)
            print(task.doc.html)
            #task.doc.save(save_path)

        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: 
    python3 cruzer_chrome.py\
    --config Crawl.MaxWorkers:1\
    --config Crawl.PageTimeout:20\
    --config Fetcher:ChromeMode:1
    '''
    Cruzer.run()

    #misc()
