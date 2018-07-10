#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
from tqdm import tqdm

import cocrawler
from cocrawler.task import Task, Req


class CounterBar():
    def __init__(self,ls,mininterval=0.1):
        """

        :param ls:
        :param mininterval: in seconds
        """
        if isinstance(ls,list):
            self.ls_max = len(ls)
        if isinstance(ls,int):
            self.ls_max = ls

        self.current = 0
        self.pbar = tqdm(total=self.ls_max,leave=False,mininterval=mininterval)

    def count(self):
        self.pbar.update(1)

def dispatcher():

    path = pathlib.Path(__file__).parent.parent / 'data' / 'top-1k.txt'
    urls = [line.strip() for line in path.open()]
    counter = CounterBar(len(urls))

    for url in urls:
        counter.count()
        yield 'http://{0}'.format(url)
        #break

class Cruzer(cocrawler.Crawler):


    def task_generator(self):
        counter = 0
        for url in dispatcher():
            counter +=1
            #url = 'https://www.whoishostingthis.com/tools/user-agent/'

            req = Req(url)
            domain = req.url.hostname_without_www
            cookie = {'data':domain,'data2':'val2'}

            #req.set_cookie(cookie)
            yield Task(name='download',req=req,raw=True,counter=counter,domain=domain)

            if counter > 500:
                break

    def task_download(self,task):


        if task.doc.status  == 200:

            #print('-->: {0}: {1}'.format(task.domain,task.cookie_list()))
            #yield Task(name='second',req=req,raw=True,domain=task.domain)
            pass

        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass


    def task_second(self,task):
        if task.doc.status  == 200:
            print('--> 222: {0}: {1}'.format(task.domain,task.cookie_list()))
        else:
            #print('--> bad code in second: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: --config Crawl.MaxWorkers:15 --loglevel INFO --reuse_session
    '''
    Cruzer.run()
