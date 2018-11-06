#!/usr/bin/env python

'''
Cruzer crawler
'''
from pathlib import Path
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
from collections import defaultdict

download_folder = Path(__file__).parent / 'archive'


LOGGER = logging.getLogger(__name__)


def isgood_link(url, domain, tag=None):

    yurl = yURL(url)

    if yurl.is_absolute():
        # if url is absulute do not download external stuff
        if yurl.host != domain:
            return False

    if yurl.path_qs == '/':
        # filter out home page and broken stuff
        return False

    return True

def save_path(url, domain):

    yurl = yURL(url)
    if yurl.is_absolute():
        rel = yurl.relative()
    else:
        rel = yurl


    download_folder_domain = download_folder / domain
    #same as rel.name ==''

    if len(rel.query) == 0 and (rel.parts[-1] == '' or  '.' not in rel.name):
        # means path ends with / or '' , threrefore create separate folder for this path and place index.html
        parts = [p for p in rel.parts if p != '/']
        parts.append('index.html')
        path = download_folder_domain.joinpath(*parts)

    else:
        if '/' in rel.query_string:
            new_query = rel.query_string.replace('/', '~')
            rel = rel.with_query(new_query)

        parts = [x for x in rel.path_qs.split('/')]

        path = download_folder_domain.joinpath(*parts)

    return path


def dispatcher():

    return [('iklan-kilat.com', '168.235.82.193'), ]



class Cruzer(cocrawler.Crawler):

    processed = defaultdict(set)


    def is_processed(self, domain, url):
        urls = self.processed.get(domain)
        if urls is None:
            return False

        if url in urls:
            return True
        else:
            return False

    def add_processed(self, domain, url):
        self.processed[domain].add(url)


    def task_generator(self):
        counter = 0
        dis = dispatcher()

        for domain, ip in dis:


            url = f'http://{ip}'
            req = Req(url)
            req.update_headers({'Host':f'{domain}'})

            yield Task(name='download',req=req,domain=domain, ip=ip, run_burner=True)


    async def task_download(self,task):

        if task.doc.status  == 200:

            if task.doc.burner:
                links = task.doc.burner.get('links')
                embeds = task.doc.burner.get('embeds')

                all_links = set()
                if len(links):
                    all_links = links
                if len(embeds):
                    all_links = all_links.union(embeds)

                if all_links:
                    counter = 0
                    counter_skipped = 0
                    for link in all_links:

                        if not isgood_link(link, task.domain):
                            continue

                        if not self.is_processed(task.domain, link):
                            counter +=1
                            yurl = yURL(link)
                            url = f'http://{task.ip}{yurl.path_qs}'
                            req = Req(url)
                            req.update_headers({'Host':f'{task.domain}'})

                            self.add_processed(task.domain, link)

                            yield Task(name='download',req=req, domain=task.domain, ip=task.ip, run_burner=True)

                        else:
                            counter_skipped +=1

                    LOGGER.info('--> [New] links added: {0}, skipped: {1}'.format(counter, counter_skipped))

            path = save_path(task.req.url.url, task.domain)
            task.doc.save(path)
            LOGGER.info('--> [OK], saved: {0}'.format(path))


def misc():


    print(save_path('http://iklan-kilat.com/wp-content/themes/robmag2/thumb.php?dd=1','iklan-kilat.com'))


if __name__ == '__main__':
    '''
    command line args example: 
    python3 cruzer.py\
    --config Crawl.MaxWorkers:10\
    --loglevel INFO\
    --reuse_session
    '''
    Cruzer.run()

    #misc()





