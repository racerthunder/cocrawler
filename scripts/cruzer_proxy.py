#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from tqdm import tqdm
from subprocess import check_output
import time
import asyncio


import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL
from cocrawler.proxy import CruzerProxy, TaskProxy, ProxyChecker, BadProxySignal
from _BIN.tools.logs import Counter


class Dispatcher():

    def __init__(self):

        self.selector = ['aaaa','aqaa']

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

class Cruzer(CruzerProxy):

    proxy_task_status = TaskProxy() # do not use task_proxy for name
    cond_html = ('____q' in proxy_task_status.doc.html) # validation condition

    checker_status = ProxyChecker(*proxy_task_status.get_cmd(),
                                  condition=any,
                                  apply_for_task='all'
                                  )

    proxy_task_body = TaskProxy(need=False) # reverse decision to false, no 'body' should be in html in this example
    body_exp = ('body222' in proxy_task_status.doc.html) # validation condition

    checker_body = ProxyChecker(*proxy_task_body.get_cmd(),
                                  condition=any,
                                  apply_for_task=['task_download']
                                  )

#class Cruzer(cocrawler.Crawler):

    def task_generator(self):

        dis = Dispatcher()
        counter = Counter(dis.total, report_every=10)

        for host in dis:
            url = 'http://{0}'.format(host)
            url = 'https://www.google.com'
            counter.count()
            proxy_url = self.proxy_url(url)
            req = Req(proxy_url)
            domain = req.url.hostname_without_www

            yield Task(name='download',req=req,domain=domain)
            break

    async def task_download(self,task):
        '''
        new proxy can be rotated directly from task by returning:
        return BadProxySignal('--> Proxy is dead bla bla')
        '''
        if task.doc.status  == 200:

            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            print(task.host_ip)
        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: 
    python3 cruzer.py\
    --config Crawl.MaxWorkers:3\
    --config Crawl.CPUControl:False\
    --config Crawl.DumpMemory:True\
    --loglevel INFO\
    --reuse_session
    '''
    Cruzer.run()

    #misc()





