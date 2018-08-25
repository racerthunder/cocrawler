from unittest import mock

import cocrawler.config as config
import cocrawler.dns as dns
import aiodns
import argparse
import collections.abc
import pickle
import logging
import asyncio

from pathlib import Path
import datetime

ARGS = argparse.ArgumentParser(description='Cruzer web crawler')

ARGS.add_argument('--loglevel', action='store', default='DEBUG')
ARGS.add_argument('--reuse_session',action='store_true')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--load', action='store',help='load previous state of the parser')

args = ARGS.parse_args()

config.config(args.configfile, args.config)

logging.basicConfig(level=logging.NOTSET)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(level=logging.INFO)

def setup_resolver(ns):
    global res
    res = aiodns.DNSResolver(nameservers=ns, rotate=True)


async def query(host, qtype):
    '''
    Use aiodns.query() to fetch dns info

    Example results:

    A: [ares_query_simple_result(host='172.217.26.206', ttl=108)]
    AAAA: [ares_query_simple_result(host='2404:6800:4007:800::200e', ttl=299)]
    NS: [ares_query_ns_result(host='ns2.google.com', ttl=None),
         ares_query_ns_result(host='ns4.google.com', ttl=None),
         ares_query_ns_result(host='ns1.google.com', ttl=None),
         ares_query_ns_result(host='ns3.google.com', ttl=None)]
    CNAME: ares_query_cname_result(cname='blogger.l.google.com', ttl=None)

    Alas, querying for A www.blogger.com doesn't return both the CNAME and the next A, just the final A.
    dig shows CNAME and A. aiodns / pycares doesn't seem to ever show the full info.
    '''
    if not res:
        raise RuntimeError('no nameservers configured')

    return await res.query(host, qtype)


class Warmupper():

    def __init__(self,loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.warmup_log_name = config.read('Fetcher','DNSWarmupLog')
        self.warmup_interval_name = config.read('Fetcher','DNSWarmupInterval')
        self.warmup_domain = config.read('Fetcher','DNSWarmupDomain')
        self.min_alive = config.read('Fetcher','DNSMinAlive')
        self.dns_timeout = config.read('Fetcher','DNSTimeout')

        self.config_dir =  Path('/Volumes/crypt/_Coding/PYTHON/cocrawler/data') # Path(__file__).parent.parent / 'data'
        self.ns_files = config.read('Fetcher', 'Nameservers')
        self.warmup_log_path = self.config_dir / self.warmup_log_name

        self.ns_list = self.load_ns()
        self.data = [None,None] # [time_last_check,alive_ns]
        self.read()
        self.today = datetime.datetime.today()


    def load_ns(self):
        file_names = self.ns_files.get('File')
        ns_list  = []
        for file_name in file_names:
            full_file_path = self.config_dir / file_name

            if not full_file_path.exists():
                raise ValueError('--> No dns file found: {0}'.format(str(full_file_path)))

            ls = [line.strip() for line in full_file_path.open(encoding='utf-8') if len(line)>1 and '#' not in line]
            ns_list.extend(ls)

        return ns_list

    def read(self):
        if self.warmup_log_path.exists():

            with open(self.warmup_log_path,'rb') as f:
                self.data[0] = pickle.load(f)
                self.data[1] = pickle.load(f)

    def save(self,path,time,good):
        with open(path,'wb') as f:
            pickle.dump(time,f)
            pickle.dump(good,f)


    async def resolve(self,ns):
        host = self.warmup_domain
        dns.setup_resolver([ns])
        try:
            result = await dns.query(host, 'A')
            LOGGER.debug('--> OK, {0}:{1}'.format(host,result))
        except Exception as ex:
            result = None
            LOGGER.error('saw exception: {0}, ns = {1}'.format(ex,ns))
        return ns,result

    async def runner(self):
        tasks = []
        good = []

        for ns in self.ns_list:
            tasks.append((ns,asyncio.Task(self.resolve(ns))))

        for ns,t in tasks:
            try:
                ns,res = await asyncio.wait_for(t,self.dns_timeout)
                if res:
                    good.append(ns)

            except asyncio.TimeoutError:
                LOGGER.error('--> Ns timeout after {0} seconds, ns: {1}'.format(self.dns_timeout,ns))

        good = set(good)
        bad = set(self.ns_list).difference(good)

        return good,bad

    def looper(self):

        goods = self.loop.run_until_complete(self.run())
        LOGGER.info('--> Dns warmup complete')
        return goods


    async def run(self):
        goods = None
        last_check = self.data[0]
        rewrite = False
        if last_check is not None:

            delta_days = (self.today - last_check).days
            if delta_days > self.warmup_interval_name:
                # run check
                LOGGER.info('--> Warmup is needed, delta: {0}'.format(delta_days))
                goods,bads = await self.runner()
                rewrite = True
            else:
                LOGGER.info('--> Warmup is fresh, delta: {0}'.format(delta_days))
                goods,bads = self.data[1]

        else:
            LOGGER.info('--> warmup needed, no log file found')
            # run check
            goods,bads = await self.runner()
            rewrite = True

        if len(goods) < self.min_alive:
            raise ValueError('--> not enough alive ns, total: {0}, required: {1}'.format(len(goods),self.min_alive))

        if rewrite:
            self.save(str(self.warmup_log_path),self.today,goods)

        return goods

def main():
    wm = Warmupper()
    goods = wm.looper()
    print(goods)




if __name__ == '__main__':
    #  --config Fetcher.Nameservers:1.1.1.1 --config Crawl.MaxDepth:30 --loglevel INFO --reuse_session
    main()
