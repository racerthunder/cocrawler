#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib


import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from tqdm import tqdm



path = pathlib.Path(__file__).resolve().parent.parent / 'data' / 'top-1k.txt'

TOTAL = sum([1 for x in path.open()])

def dispatcher():

    # ls = ['http://facebook.com','http://fbcdn.net']
    # counter.init(len(ls))
    # for url in ls:
    #     yield url
    #
    # return


    urls = [line.strip() for line in path.open()]


    for url in urls:
        yield 'http://{0}'.format(url)

        #break

class Cruzer(cocrawler.Crawler):


    def task_generator(self):
        counter = 0
        dis = dispatcher()


        for url in tqdm(dis,total=TOTAL):

            counter +=1
            #url = 'https://asdfasdlkfjasasdfasdfasdf.com'

            req = Req(url)
            domain = req.url.hostname_without_www
            #cookie = {'data':domain,'data2':'val2'}

            #req.set_cookie(cookie)
            yield Task(name='download',req=req,counter=counter,domain=domain)

            #break
            #
            if counter > 100:
                break

    def task_download(self,task):



        if task.doc.status  == 200:
            print('good: {0} , last_url: {1}'.format(task.domain,task.last_url))
            #print(vars(req))
            req = Req(task.last_url + '?id=1')
            #yield Task(name='second',req=req)

        else:
            #print('--> bad code: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            print('bad: {0}, error: {1}'.format(task.domain,task.doc.status))


    def task_second(self,task):
        if task.doc.status  == 200:
            print('good222: {0} , last_url: {1}'.format(task.req.url.hostname,task.last_url))
        else:
            print('--> bad code in second: {0}, last_exception: {1}'.format(task.last_url,task.doc.status))
            pass


if __name__ == '__main__':
    '''
    command line args example: --config Crawl.MaxWorkers:15 --loglevel INFO --reuse_session
    '''
    Cruzer.run()



