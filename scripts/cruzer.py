#!/usr/bin/env python

'''
Cruzer crawler
'''

import argparse
import cocrawler
from cocrawler.task import Task



ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store', default='/Volumes/crypt/_Coding/PYTHON/cocrawler/configs/main.yml')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--loglevel', action='store', default='INFO')
ARGS.add_argument('--load', action='store')




def dispatcher():
    import pathlib
    #urls = ['http://tut.by','http://mail.ru','http://habr.com']
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
            yield Task(name='download',url=url,raw=True,counter=counter)

            if counter > 100:
                break

    def task_download(self,task):
        if task.doc.status  == 200:
            #print(task.html)
            print('--> status good: {0}'.format(task.url.url))
            if task.doc.html:
                print('--> html len = {0}'.format(len(task.doc.html)))
            else:
                print('--> doc is empty: {0}'.format(task.url.url))

        else:
            print('--> bad code: {0}, last_exception: {1}'.format(task.url.url,task.doc.status))



Cruzer.run(ARGS)
