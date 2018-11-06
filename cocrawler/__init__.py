'''
The actual web crawler
'''

import time
import os
import random
import socket
from pkg_resources import get_distribution, DistributionNotFound
from setuptools_scm import get_version
import json
import traceback
import concurrent
import functools
import argparse
from collections import namedtuple
import inspect
import uuid

import sys
import resource
import os
import faulthandler
import gc
import warnings
import io

import asyncio
import uvloop
import logging
import aiohttp
import aiohttp.resolver
import aiohttp.connector
import psutil
import objgraph
from concurrent.futures import ThreadPoolExecutor
import async_timeout

from . import scheduler
from . import stats
from . import seeds
from . import datalayer
from . import robots
from . import parse
from . import fetcher
from . import useragent
from . import burner
from . import url_allowed
from . import post_fetch
from . import config
from . import warc
from . import dns
from . import geoip
from . import memory
from . import log_master

from . import stats
from . import timer
from . import webserver

from . sessions import SessionPool
from . dns_warmup import Warmupper



LOGGER = logging.getLogger(__name__)
__title__ = 'cocrawler'
__author__ = 'Greg Lindahl and others'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016-2017 Greg Lindahl and others'

faulthandler.enable()

class FixupEventLoopPolicy(uvloop.EventLoopPolicy):
    '''
    pytest-asyncio is weird and hijacking new_event_loop is one way to work around that.
    https://github.com/pytest-dev/pytest-asyncio/issues/38
    '''
    def new_event_loop(self):
        if self._local._set_called:
            # raise RuntimeError('An event loop has already been set')
            loop = super().get_event_loop()
            if loop.is_closed():
                loop = super().new_event_loop()
            return loop
        return super().new_event_loop()

class CallbackHandler():

    def __init__(self,partial):
        self.partial = partial

    def __await__(self):
        return self.worker().__await__()

    async def worker(self):
        await self.partial()
        return await asyncio.sleep(0.1)

class Crawler:
    def __init__(self, reuse_session=False,load=None, no_test=False, paused=False):

        self.cpu_control_worker = None
        self.mode = 'cruzer'
        self.test_mode = False # if True the first response from fetcher is cached and returned for all
        self.reuse_session = reuse_session

        self.CONFIG_TARGET_CPU_RANGE = list(range(40,70))
        self.cleanup_ssl_every = 10000 # forcelly call _cleanup_closed() on Connector
        # subsequent queries
        asyncio.set_event_loop_policy(FixupEventLoopPolicy())
        self.loop = asyncio.get_event_loop()
        self.ns_alive = Warmupper(self.loop).looper()
        self.burner = burner.Burner('parser')
        self.stopping = False
        self.paused = paused
        self.no_test = no_test
        self.next_minute = 0
        self.next_hour = time.time() + 1800
        self.max_page_size = int(config.read('Crawl', 'MaxPageSize'))
        self.prevent_compression = config.read('Crawl', 'PreventCompression')
        self.upgrade_insecure_requests = config.read('Crawl', 'UpgradeInsecureRequests')
        self.max_workers = int(config.read('Crawl', 'MaxWorkers'))
        self.workers = []
        self.init_generator = self.task_generator()
        self.init_urls_loaded = False # set to True once all urls from init list are consumed
        self.deffered_queue = asyncio.Queue()
        self.pool = SessionPool() # keep all runnning sessions if reuse_session==True
        self.log_master = log_master.LogMaster()

        #
        # try:
        #     # this works for the installed package
        #     self.version = get_distribution(__name__).version
        # except DistributionNotFound:
        #     # this works for an uninstalled git repo, like in the CI infrastructure
        #     self.version = get_version(root='..', relative_to=__file__)
        #
        self.warcheader_version = '0.99'

        self.version = '0.1' # workaround for cli running setup
        self.robotname, self.ua = useragent.useragent(self.version)


        self.resolver = dns.get_resolver(self.ns_alive)

        geoip.init()

        proxy = config.read('Fetcher', 'ProxyAll')
        if proxy:
            raise ValueError('proxies not yet supported')

        # TODO: save the kwargs in case we want to make a ProxyConnector deeper down
        self.conn_kwargs = {'use_dns_cache': False,
                            'resolver': self.resolver,
                            'limit': self.max_workers * 2,
                            'enable_cleanup_closed': True, # works together with self.connector._cleanup_closed()
                            #'verify_ssl':False,
                            #'force_close': True  # TODO: for test puprose
                            }

        local_addr = config.read('Fetcher', 'LocalAddr')
        if local_addr:
            self.conn_kwargs['local_addr'] = (local_addr, 0)
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option -- this is ipv4 only

        if self.reuse_session:
            self.conn_kwargs['force_close']=True

        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
        #TODO: https://github.com/aio-libs/aiohttp/issues/1767
        #conn._cleanup_closed_disabled = True # for testing
        self.connector = conn

        connect_timeout = config.read('Crawl', 'ConnectTimeout')
        page_timeout = float(config.read('Crawl', 'PageTimeout'))
        timeout_kwargs = {}
        if connect_timeout:
            timeout_kwargs['sock_connect'] = connect_timeout
        if page_timeout:
            timeout_kwargs['total'] = page_timeout

        self.timeout = aiohttp.ClientTimeout(**timeout_kwargs)

        if self.reuse_session is False:

            cookie_jar = aiohttp.DummyCookieJar()

            _session = aiohttp.ClientSession(connector=self.connector,
                                             cookie_jar=cookie_jar,
                                             auto_decompress=False,
                                             timeout=self.timeout
                                             )

            self.pool.global_session=_session


        self.datalayer = datalayer.Datalayer(self)
        self.robots = robots.Robots(self.robotname, 'dummy_session', self.datalayer)
        self.scheduler = scheduler.Scheduler(self.max_workers,self.robots)

        self.crawllog = config.read('Logging', 'Crawllog')
        if self.crawllog:
            self.crawllogfd = open(self.crawllog, 'a')
        else:
            self.crawllogfd = None

        self.frontierlog = config.read('Logging', 'Frontierlog')
        if self.frontierlog:
            self.frontierlogfd = open(self.frontierlog, 'a')
        else:
            self.frontierlogfd = None

        self.rejectedaddurl = config.read('Logging', 'RejectedAddUrllog')
        if self.rejectedaddurl:
            self.rejectedaddurlfd = open(self.rejectedaddurl, 'a')
        else:
            self.rejectedaddurlfd = None

        self.facetlog = config.read('Logging', 'Facetlog')
        if self.facetlog:
            self.facetlogfd = open(self.facetlog, 'a')
        else:
            self.facetlogfd = None

        self.warcwriter = warc.setup(self.version, self.warcheader_version, local_addr)

        url_allowed.setup()

        if load is not None:
            self.load_all(load)
            LOGGER.info('after loading saved state, work queue is %r urls', self.scheduler.qsize())
            LOGGER.info('at time of loading, stats are')
            stats.report()
        else:
            if self.mode != 'cruzer':
                self._seeds = seeds.expand_seeds_config(self)
                LOGGER.info('after adding seeds, work queue is %r urls', self.scheduler.qsize())
                stats.stats_max('initial seeds', self.scheduler.qsize())

        #self.stop_crawler = os.path.expanduser('~/STOPCRAWLER.{0}'.format(os.getpid()))
        self.stop_crawler = os.path.expanduser('~/STOPCRAWLER')
        LOGGER.info('Touch %s to stop the crawler.', self.stop_crawler)

        self.pause_crawler = os.path.expanduser('~/PAUSECRAWLER.{0}'.format(os.getpid()))
        LOGGER.info('Touch %s to pause the crawler.', self.pause_crawler)

        block = getattr(self,'block',None) # exists in multicore
        if block:
            self.memory_crawler = os.path.expanduser('~/MEMORYCRAWLER.{0}'.format(block))
        else:
            #self.memory_crawler = os.path.expanduser('~/MEMORYCRAWLER.{0}'.format(os.getpid()))
            self.memory_crawler = os.path.expanduser('~/MEMORYCRAWLER')

        LOGGER.info('Use %s to debug objects in the crawler.', self.memory_crawler)

        fetcher.establish_filters()

    def __del__(self):
        if hasattr(self, 'connector'):
            self.connector.close()

    def shutdown(self, message=None):
        if message is not None:
            stats.report_messages.append(message)

        self.stopping = True
        stats.coroutine_report()
        self.cancel_workers()


    @property
    def proxy_mode(self):
        from . proxy import CruzerProxy
        if isinstance(self,CruzerProxy):
            return True
        else:
            return False

    @property
    def seeds(self):
        return self._seeds

    @property
    def qsize(self):
        return self.scheduler.qsize()

    def log_rejected_add_url(self, url, reason):
        if self.rejectedaddurlfd:
            log_line = {'url': url.url, 'reason': reason}
            print(json.dumps(log_line, sort_keys=True), file=self.rejectedaddurlfd)

    def log_frontier(self, url):
        if self.frontierlogfd:
            print(url.url, file=self.frontierlogfd)


    async def add_url(self, priority, ridealong, rand=None):
        # XXX eventually do something with the frag - record as a "javascript-needed" clue

        # XXX optionally generate additional urls plugin here
        # e.g. any amazon url with an AmazonID should add_url() the base product page
        # and a non-homepage should add the homepage
        # and a homepage add should add soft404 detection
        # and ...

        url = ridealong['task'].req.url

        if self.mode != 'cruzer':
            if 'seed' in ridealong:
                seeds.seed_from_redir(url)

        # XXX allow/deny plugin modules go here
        if self.mode !='cruzer':
            if not self.robots.check_cached(url):
                reason = 'rejected by cached robots'
                stats.stats_sum('add_url '+reason, 1)
                self.log_rejected_add_url(url, reason)
                return
        # --> end skip section

        reason = None

        '''
        allowed = url_allowed.url_allowed(url)
        if not allowed:
            reason = 'rejected by url_allowed'
        elif allowed.url != url.url:
            LOGGER.debug('url %s was modified to %s by url_allow.', url.url, allowed.url)
            stats.stats_sum('add_url modified by url_allowed', 1)
            url = allowed
            ridealong['task'].req.url = url

        
        if reason:
            pass
        elif priority > int(config.read('Crawl', 'MaxDepth')):
            reason = 'rejected by MaxDepth'
        elif 'skip_crawled' not in ridealong and self.datalayer.seen(url):
            reason = 'rejected by crawled'
        elif not self.scheduler.check_budgets(url):
            # the budget is debited here, so it has to be last
            reason = 'rejected by crawl budgets'

        if 'skip_crawled' in ridealong:
            self.log_frontier(url)
        elif not self.datalayer.seen(url):
            self.log_frontier(url)

        if reason:
            stats.stats_sum('add_url '+reason, 1)
            self.log_rejected_add_url(url, reason)
            LOGGER.debug('add_url no, reason %s url %s', reason, url.url)
            return

        if 'skip_crawled' in ridealong:
            del ridealong['skip_crawled']
        '''
        # end allow/deny plugin

        LOGGER.debug('actually adding url %s, for task domain: %s', url.url, url.hostname)
        stats.stats_sum('added urls', 1)

        ridealong['priority'] = priority

        # to randomize fetches
        # already set for a freeredir
        # could be used to sub-prioritize embeds
        if rand is None:
            rand = random.uniform(0, 0.99999)

        work_id = str(uuid.uuid4())
        self.scheduler.set_ridealong(work_id, ridealong)
        await self.scheduler.queue_work((priority, rand, work_id))

        self.datalayer.add_seen(url)
        return 1

    def cancel_workers(self):
        for w in self.workers:
            if not w.done():
                w.cancel()
        # cw = self.control_limit_worker
        # if cw and not cw.done():
        #     cw.cancel()


        if self.cpu_control_worker and not self.cpu_control_worker.done():
            self.cpu_control_worker.cancel()

        if self.producer and not self.producer.done():
            self.producer.cancel()

        if self.deffered_queue_checker and not self.deffered_queue_checker.done():
            self.deffered_queue_checker.cancel()

    def on_finish(self):
        # this is the last method is called before exiting program
        # make external methods calls here, since crawler calls sys 'exit'
        pass

    async def close(self):
        stats.report()
        memory.print_summary(self.memory_crawler)
        parse.report()
        stats.check(no_test=self.no_test)
        stats.check_collisions()
        if self.crawllogfd:
            self.crawllogfd.close()
        if self.rejectedaddurlfd:
            self.rejectedaddurlfd.close()
        if self.facetlogfd:
            self.facetlogfd.close()
        if self.frontierlogfd:
            self.frontierlogfd.close()
        if self.scheduler.qsize():
            LOGGER.warning('at exit, non-zero qsize=%d', self.scheduler.qsize())

        if self.reuse_session is False:
            await self.pool.global_session.close()

        self.log_master.close_all()

    async def _retry_if_able(self, work, ridealong):

        priority, rand, surt = work
        retries_left = int(ridealong.get('retries_left', 0)) - 1
        if retries_left <= 0:
            # XXX jsonlog hard fail
            # XXX remember that this host had a hard fail
            stats.stats_sum('retries completely exhausted', 1)

            LOGGER.debug('--> retries completely exhausted = {0}, surt: {1}, domain: {2} '.format(
                retries_left, surt, ridealong['task'].req.url.hostname
            ))
            #seeds.fail(ridealong, self)
            self.scheduler.del_ridealong(surt)
            return 'no_retries_left'

        LOGGER.debug('--> retrying work: {0}'.format(work))
        # XXX jsonlog this soft fail

        # increment random so that we don't immediately retry
        extra = random.uniform(0, 0.2)
        priority, rand = self.scheduler.update_priority(priority, rand+extra)

        ridealong['priority'] = priority
        ridealong['retries_left'] = retries_left

        await self.add_url(priority,ridealong,rand=rand)
        return ridealong

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, rand, surt = work

        # when we're in the dregs of retried urls with high rand, don't exceed priority+1
        stats.stats_set('priority', priority+min(rand, 0.99))

        ridealong = self.scheduler.get_ridealong(surt)

        if 'task' not in ridealong:
            #TODO: it must be the thing when multiple workers get redirect to the same location,
            # the first one deletes surt and the others cant find it
            #raise ValueError('missing ridealong for surt '+surt)
            return None

        url = ridealong['task'].req.url

        seed_host = ridealong.get('seed_host')
        if seed_host and ridealong.get('seed'):
            robots_seed_host = seed_host
        else:
            robots_seed_host = None

        req_headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, self)

        host_geoip = {}

        entry = None

        if not mock_url:
            try:
                with async_timeout.timeout(int(config.read('Crawl', 'DnsTimeout'))):
                    entry = await dns.prefetch(url, self.resolver)

            except asyncio.TimeoutError:
                LOGGER.debug('--> DNS timeout for url: {0}'.format(url.url))
                entry = None

            else:
                if not entry:
                    LOGGER.debug('--> DNS prefectch is empty for url: {0}'.format(url.url))

            if not entry:

                # fail out, we don't want to do DNS in the robots or page fetch
                res = await self._retry_if_able(work, ridealong)

                if res == 'no_retries_left':
                    fr_dummy = namedtuple('fr_dummy','response last_exception')
                    fr_dummy.response = None # required here
                    fr_dummy.last_exception = 'dns_no_entry'
                    await self.make_callback(ridealong,fr_dummy)

                    return ridealong['task']
                else:
                    # job requeued
                    return ridealong['task']

            addrs, expires, _, host_geoip = entry
            if not host_geoip:
                with stats.record_burn('geoip lookup'):
                    geoip.lookup_all(addrs, host_geoip)
                post_fetch.post_dns(addrs, expires, url, self)



        # ---> brc, skip section <---
        # if not self.mode == 'cruzer':
        #     r = await self.robots.check(url, host_geoip=host_geoip, seed_host=robots_seed_host, crawler=self,
        #                                 headers=req_headers, proxy=proxy, mock_robots=mock_robots)
        #     if not r:
        #         # really, we shouldn't retry a robots.txt rule failure
        #         # but we do want to retry robots.txt failed to fetch
        #         self._retry_if_able(work, ridealong)
        #         return
        # ---> end skip section <--

        _session = self.pool.get_session(ridealong['task'].session_id)


        # f = NamedTuple
        f = await fetcher.fetch(url, _session, req=ridealong['task'].req,
                                max_page_size=self.max_page_size,headers=req_headers,
                                proxy=proxy, mock_url=mock_url,dns_entry=entry)


        json_log = {'kind': ridealong['task'].req.method, 'url': url.url, 'priority': priority,
                    't_first_byte': f.t_first_byte, 'time': time.time()}
        if seed_host:
            json_log['seed_host'] = seed_host
        if f.is_truncated:
            json_log['truncated'] = f.is_truncated

        if f.last_exception is not None or f.response.status >= 500:
            res = await self._retry_if_able(work, ridealong)

            # an error occured therefore no point in parsing html, make callback now and return

            if res == 'no_retries_left':
                # all retries attempts exausted, pass an error to handler
                await self.make_callback(ridealong,f)


                return ridealong['task']
            else:
                # still has some retries left, res is an old ridealong that was requed
                return res['task']



        # if f.response.status >= 400 and 'seed' in ridealong:
        #     seeds.fail(ridealong, self)

        json_log['status'] = f.response.status

        if post_fetch.is_redirect(f.response):
            __ridealong = await post_fetch.handle_redirect(f, url, ridealong, priority, host_geoip, json_log, self, rand=rand)
            # ridealong should be dict otherwise its an error
            if not isinstance(__ridealong, dict):
                fr_dummy = namedtuple('fr_dummy','response last_exception')
                fr_dummy.response = None # required here
                fr_dummy.last_exception = __ridealong
                await self.make_callback(ridealong,fr_dummy)

            else:
                __ridealong['from_redir']= True

                self.add_deffered_task(0,__ridealong)



        else:

            # all redirects already happend, get to callback function

            if f.response.status == 200:

                if ridealong['task'].run_burner:
                    html, content_data, burner_result = await post_fetch.post_200(f, url, priority,
                                                                                  host_geoip,seed_host, json_log,
                                                                                  self, run_bunner=True)
                    if burner_result:
                        links, embeds, sha1, facets, base = burner_result

                        linkset = set(u.url for u in links)
                        embedset = set(u.url for u in embeds)

                        ridealong['task'].doc.burner = {'links':linkset, 'embeds': embedset, 'base': base }

                else:

                    html, content_data, _ = await post_fetch.post_200(f, url, priority, host_geoip,
                                                                   seed_host, json_log, self)

                ridealong['from_redir']= False
                ridealong['task'].doc.html = html
                ridealong['task'].doc.content_data = content_data

            await self.make_callback(ridealong,f)



        LOGGER.debug('--> Size: [work queue]={0}, [ridealong]={1}, [deffered]={2}'.format(
                                                                    self.scheduler.qsize(),                                                                                      self.scheduler.ridealong_size(),                                                                                         self.deffered_queue.qsize()
                                                                                         ))


        stats.stats_set('queue size', self.scheduler.qsize())
        stats.stats_max('max queue size', self.scheduler.qsize())

        stats.stats_set('deffered queue size', self.deffered_queue.qsize())
        stats.stats_max('max deffered queue size', self.deffered_queue.qsize())

        stats.stats_set('ridealong size', self.scheduler.ridealong_size())

        if self.crawllogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)


        return ridealong['task']

    async def make_callback(self,ridealong,f):
        #partial = functools.partial(self.load_task_function, ridealong, f)
        with stats.record_burn('--> cruzer callback burn "{0}"'.format(ridealong['task'].name)):
            try:

                await self.load_task_function(ridealong, f)

            except ValueError as e:  # if it pukes, ..
                LOGGER.info('parser raised %r', e)

            except Exception as ex:
                traceback.print_exc()


    def fill_task(self,task,fr):
        task.doc.fetcher = fr
        task.doc.status = fr.response.status if fr.response else fr.last_exception
        task.last_url = str(fr.response.url) if fr.response else None
        ip_data = self.resolver.get_cache_entry(task.req.url.hostname)

        if ip_data and len(ip_data):
            ip = ip_data[0][0].get('host',None)
            task.host_ip = ip
            
        return task

    async def load_task_function(self,ridealong,fr):
            parent_task = self.fill_task(ridealong['task'],fr)

            task_name = 'task_{0}'.format(parent_task.name)
            task_func = getattr(self,task_name,None)

            if task_func is None:
                raise ValueError('--> Cant find task in Cruzer: {0}'.format(task_name))


            try:
                if asyncio.iscoroutinefunction(task_func):
                    # no new task will be yielded, run function and return
                    f = asyncio.ensure_future(task_func(parent_task),loop=self.loop)
                    # result is not needed here, just wait for completion
                    await f

                elif inspect.isasyncgenfunction(task_func):
                    # we have a generator, load all tasks to the queue
                    async for task in task_func(parent_task):
                        if isinstance(task,StopIteration):
                        # in proxy mode if no task left StopIteration class is returned
                            LOGGER.debug('--> No task left in: {0}, for: {1}'.format(task_name,parent_task.req.url.hostname_without_www))
                            break

                        ride_along = self.generate_ridealong(task,parent_task=parent_task)
                        LOGGER.debug('--> New task generated in: {0} -> task_{1} [{2}]'.format(task_name,
                                                                                          task.name,
                                                                                          task.req.url.url))
                        self.add_deffered_task(0,ride_along)


                else:
                    raise ValueError('--> {0} is not a Coroutine or Asyncgenerator, instead = {1}'.format(task_name,type(task_func)))

            except asyncio.CancelledError:
                pass

            except Exception as ex:
                LOGGER.warning('--> [TASK GENERATOR ERROR, SEE TRACEBACK BELOW]')
                traceback.print_exc()


            if self.reuse_session:
                self.pool.add_finished_task(parent_task.session_id,parent_task.name)


    def deffered_work_done(self):

        self.deffered_queue.task_done()

    async def work(self):
        '''
        Process queue items until we run out.
        '''
        try:
            while True:

                work = await self.scheduler.get_work()
                priority, rand, surt = work
                ridealong = self.scheduler.get_ridealong(surt)
                owner_before = owner_after = None

                try:
                    if 'owner' not in ridealong.keys():
                        LOGGER.warning('--> No owner found in ridealong: {0}'.format(ridealong))

                    owner_before = ridealong['owner']

                    task = await self.fetch_and_process(work)

                    owner_after = ridealong['owner']

                except concurrent.futures._base.CancelledError:  # seen with ^C
                    pass
                # ValueError('no A records found') should not be a mystery
                except Exception as e:
                    # this catches any buggy code that executes in the main thread
                    LOGGER.error('Something bad happened working on %s, it\'s a mystery:\n%s', work[2], e)
                    traceback.print_exc()
                    # falling through causes this work item to get marked done, and we continue

                #we track task_done for deffered queue separatelly for correct join

                self.scheduler.work_done()
                # only here we can say confidentially that any task has been completed after
                # fetch_and_process is done

                is_deffered_owner = ((owner_before == owner_after) & (owner_after == 'deffered_queue'))

                if is_deffered_owner:
                    stats.stats_sum('deffered_done',1)
                    self.deffered_work_done()


                self.scheduler.del_ridealong(surt)


                if self.stopping:
                    raise asyncio.CancelledError

                if self.paused:
                    with stats.coroutine_state('paused'):
                        while self.paused:
                            await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass
        except Exception as ex:
            traceback.print_exc()
            raise ex

    def _calculate_percent_delta(self,num1, num2):
        percent = round((float(num1)/float(num2))*100)
        return percent

    async def control_cpu_usage(self):

        current_process = psutil.Process(os.getpid())
        cpu_history = [max(self.CONFIG_TARGET_CPU_RANGE)] * 5 # dummy fill

        while True:
            await asyncio.sleep(30)

            cur_cpu = current_process.cpu_percent(interval=None)
            # add cpu to recent history and calculate avgCpu (30 sec)
            cpu_history.insert(0,cur_cpu)


            # start adjusting
            del cpu_history[5:]

            avg_cpu = int(sum(cpu_history) / len(cpu_history))

            limit = max((self.max_workers * 5) // 100, 1)


            block = getattr(self, 'block', None)


            if avg_cpu > max(self.CONFIG_TARGET_CPU_RANGE):


                delta = self._calculate_percent_delta(self.scheduler.q._maxsize, self.max_workers)

                if delta < 20:
                    LOGGER.info('--> CPU NO DOWN [{0}], max queue size: {1} is less than 20% of number of workers: {2}'.format(
                        block or '',
                        self.scheduler.q._maxsize,
                        self.max_workers
                        ))
                else:
                    self.scheduler.q._maxsize -= limit

                    # decreaxe max queue size by  5%
                    LOGGER.info('--> CPU DOWN [{3}], [queue max before={0} after={1}] avg cpu={2}'.format(
                        self.scheduler.q._maxsize,
                        self.scheduler.q._maxsize-limit,
                        avg_cpu,
                        block or ''
                    ))

                    # at least 1 min  since queue to be exausted if number is down scaled
                    await asyncio.sleep(60)

            elif avg_cpu < min(self.CONFIG_TARGET_CPU_RANGE) :


                delta = self._calculate_percent_delta(self.scheduler.qsize(), self.scheduler.q._maxsize)
                # do not insrease max queue size if current queue is filled more than 80%
                if delta < 80:
                    # queue is being used ok, time to heat up
                    if self.scheduler.q._maxsize * 2 > self.max_workers:
                        LOGGER.info('--> CPU NO UP [{0}], max queue size 2 times large than num '
                                    'of workers'.format(block or ''))
                    else:
                        # increase max queue size by 5%
                        LOGGER.info('--> CPU UP [{3}], [queue max before={0} after={1}] avg cpu={2}'.format(
                            self.scheduler.q._maxsize,
                            self.scheduler.q._maxsize+limit,
                            avg_cpu,
                            block or ''
                        ))
                        self.scheduler.q._maxsize +=limit

                        # give sometiem to pick up the pace
                        await asyncio.sleep(60)
                else:
                    LOGGER.info('--> CPU NO UP [{0}], queue is filled by {1} %'.format(
                        block or '',
                        delta
                    ))
                    if delta == 100:
                        LOGGER.info('--> CPU NO UP [{0}], queue 100% filled, states = {1} %'.format(
                            block or '',
                            str(self.get_workers_state())
                        ))

            else:
                # oK within the range
                LOGGER.info('--> CPU RANGE [{0}], avg_cpu = {1} %'.format(
                    block or '',
                    avg_cpu
                ))



    async def control_limit(self):
        '''
        Worker dedicated to managing how busy we let the network get
        '''
        last = time.time()
        await asyncio.sleep(1.0)
        limit = self.max_workers//2
        dominos = 0
        undominos = 0
        while True:
            await asyncio.sleep(1.0)
            t = time.time()
            elapsed = t - last
            old_limit = limit

            if elapsed < 1.03:
                dominos += 1
                undominos = 0
                if dominos > 2:
                    # one action per 3 seconds of stability
                    limit += 1
                    dominos = 0
            else:
                dominos = 0
                if elapsed > 5.0:
                    # always act on tall spikes
                    limit -= max((limit * 5) // 100, 1)  # 5%
                    undominos = 0
                elif elapsed > 1.1:
                    undominos += 1
                    if undominos > 1:
                        # only act if the medium spike is wider than 1 cycle
                        # (note: these spikes are caused by garbage collection)
                        limit -= max(limit // 100, 1)  # 1%
                        undominos = 0
                else:
                    undominos = 0
            limit = min(limit, self.max_workers)
            limit = max(limit, 1)

            self.connector._limit = limit  # private instance variable
            stats.stats_set('network limit', limit)
            last = t

            if limit != old_limit:
                LOGGER.info('control_limit: elapsed = %f, adjusting limit by %+d to %d',
                            elapsed, limit - old_limit, limit)
            else:
                LOGGER.info('control_limit: elapsed = %f', elapsed)

    def summarize(self):
        self.scheduler.summarize()

    def save(self, f):
        self.scheduler.save(self, f, )

    def load(self, f):
        self.scheduler.load(self, f)

    def get_savefilename(self):
        savefile = config.read('Save', 'Name') or 'cocrawler-save-$$'
        savefile = savefile.replace('$$', str(os.getpid()))
        savefile = os.path.expanduser(os.path.expandvars(savefile))
        if os.path.exists(savefile) and not config.read('Save', 'Overwrite'):
            count = 1
            while os.path.exists(savefile + '.' + str(count)):
                count += 1
            savefile = savefile + '.' + str(count)
        return savefile

    def save_all(self):
        savefile = self.get_savefilename()
        with open(savefile, 'wb') as f:
            self.save(f)
            self.datalayer.save(f)
            stats.save(f)

    def load_all(self, filename):
        with open(filename, 'rb') as f:
            self.load(f)
            self.datalayer.load(f)
            stats.load(f)

    async def minute(self):
        '''
        print interesting stuff, once a minute + close all finished sessions
        '''
        if time.time() < self.next_minute:
            return

        self.next_minute = time.time() + 60
        stats.stats_set('DNS cache size', self.resolver.size())
        ru = resource.getrusage(resource.RUSAGE_SELF)
        vmem = (ru[2])/1000000.  # gigabytes
        stats.stats_set('main thread vmem', vmem)

        if config.read('Logging', 'StatsReport'):
            stats.report()
            stats.coroutine_report()

        memory.print_summary(self.memory_crawler)

        if self.reuse_session:
            await self.pool.close_or_wait()



    def hour(self):
        '''Do something once per hour'''
        if time.time() < self.next_hour:
            return

        self.next_hour = time.time() + 1800

        if config.read('Crawl', 'DumpMemory'):
            block = getattr(self,'block',None)

            if block:
                path = './MEMORY_DEBUG_{0}.txt'.format(block)
            else:
                path = './MEMORY_DEBUG.txt'

            memory.dump_summary_cruzer(path)

    def update_cpu_stats(self):
        elapsedc = time.process_time()  # should be since process start
        stats.stats_set('main thread cpu time', elapsedc)

    def generate_ridealong(self,task,parent_task=None):
        # url = Url instance

        if parent_task is not None:
            # parent_task exists only in deffered queue
            task.add_parent(parent_task.name)
            task.set_session_id(parent_task.session_id)

        else:
            # it's a init_generator, create new session and put it into the pool
            if self.reuse_session:
                _id = self.create_session(url=task.req.url.url)
                task.set_session_id(_id)

        if self.reuse_session:
            # do no track single session mode since ID for session = None (same for all requests)
            self.pool.add_submited_task(task.session_id,task.name)

        # add ref to cruzer instance so we have access to sessions pool
        task.cruzer = self

        max_tries = config.read('Crawl', 'MaxTries')
        max_redirs = config.read('Crawl', 'MaxRedirs')

        ride_along = {'task':task,'skip_seen_url':True,'retries_left': max_tries, 'redirs_left':max_redirs}
        return ride_along

    def task_generator(self):
        yield ':)'

    def add_deffered_task(self,priority, ridealong):
        #priority =  lower number = higher priority
        # add urls from redirects detection and from cruzer callback functions


        ridealong['owner'] = 'deffered_queue'

        self.deffered_queue.put_nowait((priority,ridealong))

    def create_session(self,**kwargs):
        '''
        create new session instance and add it to the pool
        connector_owner=False is important here, it creates a subclass of connector for each client
        session, with = True common connector is used and call to "close" method will result for all
        open connection to stall with "connection is closed" error

        :return: session id
        '''
        cookie_jar = aiohttp.CookieJar(unsafe=True)
        __session = aiohttp.ClientSession(connector=self.connector, cookie_jar=cookie_jar,
                                          timeout=self.timeout,auto_decompress=False,connector_owner=False)
        _id = id(__session)

        self.pool.add_session(_id,__session,**kwargs)

        return _id



    async def deffered_queue_processor(self):
        """
        this queue give controal only in 2 cases: 1. main queue is full 2. deffered queue is empty

        if there are things in deffered queue we fill main queue in one go
        therefore do not place extra sleeps here
        :return:
        """
        try:
            while True:
                try:

                    priority, ridealong = self.deffered_queue.get_nowait()

                    stats.stats_sum('deffered_get',1)

                    result = await self.add_url(priority,ridealong)

                    LOGGER.debug('--> deffered task added, work queue:{0} url [{2}]: {1}'.format(
                        self.scheduler.qsize(),
                        ridealong['task'].req.url,
                        ridealong['task'].req.method
                    ))


                except asyncio.queues.QueueEmpty:

                    LOGGER.debug('--> deffered queue is empty, get: {0}, done: {1}'.format(
                                                            stats.sums.get('deffered_get',0),
                                                            stats.sums.get('deffered_done',0))
                                                            )
                    await asyncio.sleep(1)

                except concurrent.futures._base.CancelledError:  # seen with ^C
                    pass
                except Exception as ex:
                    traceback.print_exc()
                    #break

                if self.stopping:

                    raise asyncio.CancelledError

        except asyncio.CancelledError:
            pass

        except Exception as ex:
            traceback.print_exc()
            raise ex

    async def queue_producer(self):
        while True:
            # deffered queue has priority over initail urls, that why we also include it here
            # do not try to get items from this queue here since it could be racy with main coroutine
            task = None

            while True:
                # sleep until deffered queue is empty again
                if not self.deffered_queue.empty():
                    LOGGER.debug('--> Queue producer is sleeping.')
                    await asyncio.sleep(0.5)
                else:
                    break

            try:
                task = next(self.init_generator)
                LOGGER.debug('--> new task submited [{2}]: "{0}" for {1}'.format(task.name,
                                                                               task.req.url.url,
                                                                               task.req.method))
            except StopIteration:
                LOGGER.debug('--> cruzer iter is empty')
                break

            except concurrent.futures._base.CancelledError:  # seen with ^C
                pass

            except Exception as ex:
                traceback.print_exc()
                #break

            if task is not None:
                ride_along = self.generate_ridealong(task)
                ride_along['owner'] = 'main_queue'

                await self.add_url(1,ride_along)
            else:
                LOGGER.warning('--> Queue_producer got empty task object.')

            if self.stopping:
                raise asyncio.CancelledError


    def get_workers_state(self):

        workers_alive_num = sum([1 for w in self.workers if not w.done()])
        workers_dead_num = len(self.workers) - workers_alive_num
        is_alive_deffered_queue_processor = self.deffered_queue_checker.done()
        is_alive_queue_producer = self.producer.done()

        return {'workers_alive':workers_alive_num,
                'workers_dead':workers_dead_num,
                'deffered_queue_processor_is_done':is_alive_deffered_queue_processor,
                'main_queue_producer_is_done':is_alive_queue_producer
                }


    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        #await self.minute()  # print pre-start stats

        #self.control_limit_worker = asyncio.Task(self.control_limit())

        if config.read('Crawl', 'CPUControl'):
            self.cpu_control_worker = asyncio.Task(self.control_cpu_usage())

        self.producer = asyncio.Task(self.queue_producer())


        self.workers = [asyncio.Task(self.work()) for _ in range(self.max_workers)]
        self.deffered_queue_checker = asyncio.Task(self.deffered_queue_processor())
        # this is now the 'main' coroutine

        if config.read('Multiprocess', 'Affinity'):
            # set the main thread to run on core 0
            p = psutil.Process()
            p.cpu_affinity([p.cpu_affinity().pop(0)])

        while True:
            await asyncio.sleep(1)

            if not self.stopping and os.path.exists(self.stop_crawler):
                LOGGER.warning('saw STOPCRAWLER file, stopping crawler and saving queues')
                self.stopping = True

            if not self.paused and os.path.exists(self.pause_crawler):
                LOGGER.warning('saw PAUSECRAWLER file, pausing crawler')
                self.paused = True
            elif self.paused and not os.path.exists(self.pause_crawler):
                LOGGER.warning('saw PAUSECRAWLER file disappear, un-pausing crawler')
                self.paused = False

            self.workers = [w for w in self.workers if not w.done()]
            LOGGER.debug('%d workers remain', len(self.workers))

            if len(self.workers) == 0:
                # this triggers if we've exhausted our url budget and all workers cancel themselves
                # queue will likely not be empty in this case
                LOGGER.warning('all workers exited, finishing up.')
                break

            if self.scheduler.done(len(self.workers)):
                # this is a little racy with how awaiting work is set and the queue is read
                # while we're in this join we aren't looking for STOPCRAWLER etc

                if not self.producer.done():
                    # in any case, if initial urls are not consumed do not even try to check for completion
                    # we can fall here prematurelly because of garbage collector and coroutines being too busy
                    LOGGER.info('--> Main queue is about to join, but initial urls are not consumed')
                    await asyncio.sleep(1)
                    continue

                await asyncio.sleep(1) # give a chance to defered queue to finalize

                if self.reuse_session:
                    await self.pool.close_or_wait()


                if self.pool.busy == True:

                    LOGGER.warning('--> queue is about to join, but there are tasks in submited list, '
                                   '[ main queue: {0} , deffered queue: {1} , ridealong size: {2} ]'
                                   .format(self.scheduler.qsize(),
                                           self.deffered_queue.qsize(),
                                           len(self.scheduler.ridealong)

                                           ))
                    await asyncio.sleep(1)

                else:

                    LOGGER.warning('all workers appear idle, queue appears empty, executing join')

                    for w in self.workers:
                        if w.done() and w.exception():
                            text = '\n\n' + 50*'#' + '\n{0}\n' + 50*'#' + '\n\n'
                            LOGGER.warning(text.format('Workers stopped by exception: {0}'
                                                       .format(w.exception())))


                    await self.deffered_queue.join()
                    LOGGER.warning('--> deffered queue joined succesfully, scheduler is next')
                    await self.scheduler.close()
                    break

            self.update_cpu_stats()
            # show stats and close finished sessions
            await self.minute()
            self.hour()



        self.cancel_workers()

        if self.stopping or config.read('Save', 'SaveAtExit'):
            pass
            #self.summarize()
            #self.datalayer.summarize()
            #LOGGER.warning('saving datalayer and queues')
            #self.save_all()
            #LOGGER.warning('saving done')


    @classmethod
    def run(cls):

        '''
        Main program: parse args, read config, set up event loop, run the crawler.
        '''

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

        if args.printdefault:
            config.print_default()
            sys.exit(1)

        loglevel = os.getenv('COCRAWLER_LOGLEVEL') or args.loglevel
        logging.basicConfig(level=loglevel)

        config.config(args.configfile, args.config)

        #memory.limit_resources()

        if os.getenv('PYTHONASYNCIODEBUG') is not None:
            logging.captureWarnings(True)
            warnings.simplefilter('default', category=ResourceWarning)
            if LOGGER.getEffectiveLevel() > logging.WARNING:
                LOGGER.setLevel(logging.WARNING)
                LOGGER.warning('Lowered logging level to WARNING because PYTHONASYNCIODEBUG env var is set')
            LOGGER.warning('Configured logging system to show ResourceWarning because PYTHONASYNCIODEBUG env var is set')
            LOGGER.warning('Note that this does have a significant impact on asyncio overhead')
        if os.getenv('COCRAWLER_GC_DEBUG') is not None:
            LOGGER.warning('Configuring gc debugging')
            gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_UNCOLLECTABLE)

        kwargs = {}
        if args.load:
            kwargs['load'] = args.load
        if args.no_test:
            kwargs['no_test'] = True
        if args.reuse_session:
            kwargs['reuse_session'] = True


        cruzer = cls(**kwargs)

        loop = cruzer.loop

        #loop.set_debug(True) # https://docs.python.org/3/library/asyncio-dev.html#asyncio-debug-mode

        slow_callback_duration = os.getenv('ASYNCIO_SLOW_CALLBACK_DURATION')
        if slow_callback_duration:
            loop.slow_callback_duration = float(slow_callback_duration)
            LOGGER.warning('set slow_callback_duration to %f', slow_callback_duration)

        if config.read('CarbonStats', 'Enabled'):
            timer.start_carbon()

        if config.read('REST'):
            app = webserver.make_app()
        else:
            app = None

        try:
            loop.run_until_complete(cruzer.crawl())
        except KeyboardInterrupt:
            sys.stderr.flush()
            print('\nInterrupt. Exiting cleanly.\n')
            stats.coroutine_report()
            cruzer.cancel_workers()

        finally:
            loop.run_until_complete(cruzer.close())
            if app:
                webserver.close(app)
            if config.read('CarbonStats', 'Enabled'):
                timer.close()
            # apparently this is needed for full aiohttp cleanup -- or is it cargo cult
            loop.stop()
            loop.run_forever()
            loop.close()
            cruzer.on_finish()


        exit(stats.exitstatus)
