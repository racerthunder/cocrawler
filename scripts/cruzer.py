#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import argparse
import cocrawler
from cocrawler.task import Task, Req



def dispatcher():

    path = pathlib.Path(__file__).parent.parent / 'data' / 'top-1k.txt'
    urls = [line.strip() for line in path.open()]
    for url in urls:
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

            req.set_cookie(cookie)
            yield Task(name='download',req=req,raw=True,counter=counter,domain=domain)

            if counter > 0:
                break

    def task_download(self,task):


        if task.doc.status  == 200:

            print(f'--> good: {task.last_url}')
            #yield Task(name='second',req=req,raw=True,domain=task.domain)

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
    command line args example: --config Crawl.MaxWorkers:100
    '''
    Cruzer.run()
