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
#from cocrawler.proxy import CruzerProxy, TaskProxy, ProxyChecker



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


# from _BIN.proxy import Proxy
# class Cruzer(CruzerProxy):
#     proxy = Proxy()
#
#     proxy_task_html = TaskProxy() # do not use task_proxy for name
#     validation_html = (proxy_task_html.doc.status == 500)
#
#     checker_html = ProxyChecker(*proxy_task_html.get_cmd(),
#                                 condition=any,
#                                 apply_for_task=['task_post']
#                                 )
#
#     proxy_task_status = TaskProxy() # do not use task_proxy for name
#     validation_status = (proxy_task_status.doc.status == 200)
#
#     checker_status = ProxyChecker(*proxy_task_status.get_cmd(),
#                                   condition=any,
#                                   apply_for_task=['task_last']
#                                   )

class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        counter = 0
        dis = dispatcher()
        total = get_lines_count(path)

        for url in tqdm(dis,total=total):

            counter +=1
            #url = 'http://httpbin.org/get'
            #proxy_url = self.proxy.get_next_proxy_cycle(url)
            req = Req(url)
            domain = req.url.hostname_without_www

            #cookie = {'data':domain,'data2':'val2'}
            #req.get = cookie


            yield Task(name='download',req=req,counter=counter,domain=domain)

            #time.sleep(5)
            if counter > 0:
                break

    async def task_download(self,task):

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            print(task.host_ip)
        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            pass

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





