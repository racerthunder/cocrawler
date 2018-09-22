#!/usr/bin/env python

'''
Cruzer crawler
'''
import pathlib
import logging
from tqdm import tqdm
from subprocess import check_output
import sys
from weblib.pwork import make_work
from multiprocessing import current_process, cpu_count
from setproctitle import setproctitle

import cocrawler
from cocrawler.task import Task
from cocrawler.req import Req
from cocrawler.urls import URL



path = pathlib.Path(__file__)


def get_lines_count(path):

    res = check_output(['wc','-l',str(path)])
    count = int(res.split()[0])

    return count



LOGGER = logging.getLogger(__name__)



class Dispatcher():
    cpu_count = cpu_count()
    total_links = get_lines_count(str(path))
    block_links = int(total_links / cpu_count)


    def __init__(self,block):
        self.block = block
        self.start,self.end = self.calc_start_end()
        self.sub_total = self.end - self.start

    def calc_start_end(self):
        skip = int((self.block-1) * self.__class__.block_links)
        start, end = skip, skip + self.__class__.block_links
        return start,end

    def url_dispatcher(self):
        cur_line_num = 0
        with open(str(path)) as f:
            for line in f:
                if cur_line_num<self.start:
                    cur_line_num+=1
                    continue

                if cur_line_num>=self.end and self.block != self.cpu_count:
                    break

                yield line.strip()
                cur_line_num+=1




class Cruzer(cocrawler.Crawler):

    def task_generator(self):

        block = getattr(Cruzer,'block')
        dis_instance = Dispatcher(block)
        dis = dis_instance.url_dispatcher()
        total = dis_instance.sub_total
        counter = 0
        update_title_every = 1000

        for domain in tqdm(dis,total=total,mininterval=5):
            #for url in dis:
            counter +=1
            url = 'http://{0}/'.format(domain.lower())
            req = Req(url)

            yield Task(name='download',req=req)

            if counter % update_title_every == 0:
                percent = round((float(counter)/float(total))*100)

                setproctitle('worker {0}, progress: {1} %, ({2} - {3})'.format(block,
                                                                               percent,
                                                                               dis_instance.start,
                                                                               dis_instance.end
                                                                               ))

    async def task_download(self,task):



        if task.doc.status  == 200 :
            if task.doc.html and 'token' in task.doc.html:

                log_path = pathlib.Path(__file__).parent / 'ok-found-{0}.txt'.format(getattr(Cruzer,'block'))

                self.log_master.write(str(log_path), task.last_url, close_everytime=True)


def worker(block):

    # p.cpu_affinity([block,])
    setattr(Cruzer,'block',block)

    Cruzer.run()


def load_cpu():

    cpu_workers = list(range(1,cpu_count()))
    #cpu_workers =[1,]
    for res in make_work(worker, cpu_workers, cpu_count()):
        pass


def misc():

    dis = Dispatcher(1)
    print(vars(dis))
    print('total links: ',dis.total_links)
    print('cpu: ', cpu_count())

    for url in dis.url_dispatcher():
        print(url)

if __name__ == '__main__':
    '''
    python3 dup_cruzer.py --config Crawl.MaxWorkers:200  --config Crawl.MaxTries:0 --config Crawl.AllowExternalRedir:False --config Crawl.CPUControl:False --config CarbonStats.Enabled:True --loglevel INFO 
    '''

    load_cpu()
    #misc()









