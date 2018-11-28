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



if sys.platform == 'darwin':

    path = pathlib.Path('urls.txt')

else:
    path = pathlib.Path('/root/.txt')

def get_line_index(file_path,find):

    try:
        cmd = ['grep','-n','-w', find, str(file_path)]
        res = check_output(cmd).decode('utf-8')
        if ':' not in res:
            raise ValueError('--> Line not found: {0}'.format(find))

        else:
            index = int(res.split(':')[0])

    except Exception as ex:
        print(ex)

    return index

def get_lines_count(path):

    res = check_output(['wc','-l',str(path)])
    count = int(res.split()[0])

    return count

def get_files_per_cores(mask=None):
    files = pathlib.Path(__file__).parent.glob(mask)
    if mask.startswith('ok-'):
        return dict([(f.name.split('-')[2].replace('.txt',''),f) for f in files])

    elif mask.startswith('last_url'):
        return dict([(f.name.split('-')[1].replace('.txt',''),f) for f in files])

    else:
        raise ValueError('--> Unknown mask for glob')


def get_last_line_per_core(mask=None, filter=None):
    assert mask is not None
    data = get_files_per_cores(mask=mask)
    new_data = {}
    for core,path in data.items():
        if filter:
            last_line =[line.strip() for line in path.open() if filter in line][-1]
        else:
            last_line =[line.strip() for line in path.open()][-1]

        new_data[core] = last_line

    return new_data


def get_first_line_per_core(mask=None, filter=None):
    assert mask is not None
    data = get_files_per_cores(mask=mask)
    new_data = {}
    for core,path in data.items():
        if filter:
            first_line =[line.strip() for line in  path.open() if filter in line][0]
        else:
            first_line =[line.strip() for line in  path.open()][0]

        new_data[core] = first_line

    return new_data

LOGGER = logging.getLogger(__name__)



class Dispatcher():
    cpu_count = cpu_count()
    total_links = get_lines_count(str(path))
    block_links = int(total_links / cpu_count)


    def __init__(self, block, skip_percent=None, start_line=None, end_line=None):

        if (skip_percent and start_line) or (skip_percent and end_line):
            raise ValueError('--> skip_percent and start_line/end_line cant be used together')

        self.skip_percent = skip_percent # in %, skip this block from the start
        self.start_line = start_line # skip everything before this line (skip_percent not applied)
        self.end_line = end_line # skip everything after this line (skip_percent not applied)
        self.block = block
        self.start,self.end = self.calc_start_end()
        self.sub_total = self.end - self.start

    def calc_start_end(self):
        skip = int((self.block-1) * self.__class__.block_links)
        start, end = skip, skip + self.__class__.block_links

        if self.skip_percent:
            start = start + max(((end-start) * self.skip_percent) // 100, 1)

        if self.start_line:
            start = get_line_index(str(path),self.start_line)

        if self.end_line:
            end = get_line_index(str(path),self.end_line)


        if start >=end:
            raise ValueError('--> Block [{2}], Start line ({3}) cant be below end_line ({4}) in file, '
                             'indexes: {0}:{1}'.format(start,
                                                       end,
                                                       self.block,
                                                       self.start_line or start,
                                                       self.end_line or end
                                                       ))

        sub_total = end - start

        if sub_total > self.block_links:
            raise ValueError('--> Block [{2}], Selection block is bigger than core block size: {0} : {1}'.
                             format(sub_total,
                                    self.block_links,
                                    self.block
                                    ))

        return start,end

    def url_dispatcher(self):
        cur_line_num = 0

        with open(str(path)) as f:
            for line in f:
                if cur_line_num<self.start:
                    cur_line_num+=1
                    continue

                if cur_line_num>=self.end and self.block != self.cpu_count:
                    # do not break on the last block instead reach the end of the file
                    break


                yield line.strip()

                cur_line_num+=1



class Cruzer(cocrawler.Crawler):

    def task_generator(self):

        block = getattr(Cruzer,'block')

        # -----  START FROM WHERE YOU LEFT --------
        # firsts = get_first_line_per_core(mask='last_url-*')
        # first_domain = firsts[str(block)]
        #
        # lasts = get_last_line_per_core(mask='ok-found-*')
        # last_domain = URL(lasts[str(block)]).hostname_without_www.upper()
        # -------- END ----------------

        #TODO: CHECK THIS TWICE
        dis_instance = Dispatcher(block)

        dis = dis_instance.url_dispatcher()
        total = dis_instance.sub_total
        counter = 0
        prev_percent = 0
        percent = 0
        update_title_every = 1000

        for domain in tqdm(dis,total=total,mininterval=5):
            #for domain in dis:
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

                sys.stdout.flush()

            if percent > prev_percent:
                # save url every percent change
                log_progress = pathlib.Path(__file__).parent / 'last_url-{0}.txt'.format(getattr(Cruzer,'block'))
                self.log_master.write(str(log_progress), domain, close_everytime=True)
                prev_percent = percent


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
    cpu_workers =[1,]
    for res in make_work(worker, cpu_workers, cpu_count()):
        pass


def misc():

    firsts = get_first_line_per_core(mask='last_url-*')
    first_domain = firsts['2']

    print(first_domain)

    lasts = get_last_line_per_core(mask='ok-found-*')
    last_domain = URL(lasts['2']).hostname_without_www.upper()
    print(last_domain)

    dis = Dispatcher(2)
    print(dis.start,dis.end,dis.sub_total,sep=' : ')

    dis2 = Dispatcher(2,start_line=last_domain,end_line=first_domain)
    print(dis2.start,dis2.end,dis2.sub_total,sep=' : ')

    for url in dis.url_dispatcher():
        print(url)
        break

    # print(10*'-')
    #
    # for url in dis2.url_dispatcher():
    #     print(url)
    #     break

if __name__ == '__main__':
    '''
    UPDATE CarbonStats SECTION OF CONFIG !! + upload /cocrawler/dashboard.json to admin panel
    carbon installation = https://graphite.readthedocs.io/en/latest/install.html
    
    python3 dup_cruzer.py\
    --config Crawl.MaxWorkers:200\
    --config Crawl.MaxTries:0\
    --config Crawl.AllowExternalRedir:False\
    --config Crawl.CPUControl:False\
    --config CarbonStats.Enabled:True\
    --config Crawl.DumpMemory:True\
    --config Logging.StatsReport:False\
    --loglevel INFO
    
    '''

    load_cpu()
    #misc()









