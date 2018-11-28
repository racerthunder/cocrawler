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
from yarl import URL as yURL


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



class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        counter = 0
        dis = dispatcher()
        total = get_lines_count(path)

        for url in tqdm(dis,total=total):

            counter +=1
            url = 'http://vim.org/'

            req = Req(url)
            domain = req.url.hostname_without_www

            #cookie = {'data':domain,'data2':'val2'}
            #req.get = cookie


            yield Task(name='download',req=req,domain=domain)

            #time.sleep(5)
            if counter > 0:
                break

    async def task_download(self,task):

        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))

        else:
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))
            pass

    async def task_second(self,task):
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
    --config Crawl.MaxWorkers:0\
    --config Crawl.MaxTries:3\
    --config Crawl.DumpMemory:True\
    --config Crawl.AllowExternalRedir:False\
    --loglevel INFO\
    --reuse_session
    '''
    Cruzer.run()

    #misc()





