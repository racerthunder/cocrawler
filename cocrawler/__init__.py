'''
The actual web crawler
'''

import time
import os
import random
import socket
# from pkg_resources import get_distribution, DistributionNotFound
# from setuptools_scm import get_version
import json
import traceback
import concurrent
import functools
import argparse
from collections import namedtuple
import inspect

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

from . import stats
from . import timer
from . import webserver

from . sessions import SessionPool


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
        self.partial()
        return await asyncio.sleep(0.1)

class Crawler:
    def __init__(self, reuse_session=False,load=None, no_test=False, paused=False):
        self.mode = 'cruzer'
        self.test_mode = False # if True the first response from fetcher is cached and returned for all
        self.reuse_session = reuse_session
        # subsequent queries
        asyncio.set_event_loop_policy(FixupEventLoopPolicy())
        self.loop = asyncio.get_event_loop()
        self.burner = burner.Burner('parser')
        self.stopping = False
        self.paused = paused
        self.no_test = no_test
        self.next_minute = time.time() + 60
        self.max_page_size = int(config.read('Crawl', 'MaxPageSize'))
        self.prevent_compression = config.read('Crawl', 'PreventCompression')
        self.upgrade_insecure_requests = config.read('Crawl', 'UpgradeInsecureRequests')
        self.max_workers = int(config.read('Crawl', 'MaxWorkers'))
        self.workers = []
        self.init_generator = self.task_generator()
        self.init_urls_loaded = False # set to True once all urls from init list are consumed
        self.deffered_queue = asyncio.Queue()
        self.pool = SessionPool() # keep all runnning sessions if reuse_session==True

        # try:
        #     # this works for the installed package
        #     self.version = get_distribution(__name__).version
        # except DistributionNotFound:
        #     # this works for an uninstalled git repo, like in the CI infrastructure
        #     self.version = get_version(root='..', relative_to=__file__)

        self.version = '0.1' # workaround for cli running setup
        self.robotname, self.ua = useragent.useragent(self.version)


        self.resolver = dns.get_resolver()

        geoip.init()

        proxy = config.read('Fetcher', 'ProxyAll')
        if proxy:
            raise ValueError('proxies not yet supported')

        # TODO: save the kwargs in case we want to make a ProxyConnector deeper down
        self.conn_kwargs = {'use_dns_cache': False, 'resolver': self.resolver,
                            'limit': 0,
                            'enable_cleanup_closed': True}
        local_addr = config.read('Fetcher', 'LocalAddr')
        if local_addr:
            self.conn_kwargs['local_addr'] = (local_addr, 0)
        self.conn_kwargs['family'] = socket.AF_INET  # XXX config option -- this is ipv4 only

        if self.reuse_session:
            self.conn_kwargs['force_close']=True
            self.conn_kwargs['enable_cleanup_closed'] = True


        conn = aiohttp.connector.TCPConnector(**self.conn_kwargs)
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

            _session = aiohttp.ClientSession(connector=self.connector, cookie_jar=cookie_jar,
                                             auto_decompress=False,timeout=self.timeout)

            self.pool.global_session=_session

        self.datalayer = datalayer.Datalayer()
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

        self.warcwriter = warc.setup(self.version, local_addr)

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

        self.stop_crawler = os.path.expanduser('~/STOPCRAWLER.{}'.format(os.getpid()))
        self.pause_crawler = os.path.expanduser('~/PAUSECRAWLER.{}'.format(os.getpid()))

        LOGGER.info('Touch %s to stop the crawler.', self.stop_crawler)
        LOGGER.info('Touch %s to pause the crawler.', self.pause_crawler)

    def __del__(self):
        self.connector.close()

    def shutdown(self):
        stats.coroutine_report()
        self.cancel_workers()
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


    async def add_url(self, priority, ridealong):
        # XXX eventually do something with the frag - record as a "javascript-needed" clue

        # XXX optionally generate additional urls plugin here
        # e.g. any amazon url with an AmazonID should add_url() the base product page
        # and a non-homepage should add the homepage
        # and a homepage add should add soft404 detection
        # and ...

        url = ridealong['url']

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

        allowed = url_allowed.url_allowed(url)
        if not allowed:
            reason = 'rejected by url_allowed'
        elif allowed.url != url.url:
            LOGGER.debug('url %s was modified to %s by url_allow.', url.url, allowed.url)
            stats.stats_sum('add_url modified by url_allowed', 1)
            url = allowed
            ridealong['url'] = url

        '''
        if reason:
            pass
        elif priority > int(config.read('Crawl', 'MaxDepth')):
            reason = 'rejected by MaxDepth'
        elif 'skip_crawled' not in ridealong and self.datalayer.crawled(url):
            reason = 'rejected by crawled'
        elif not self.scheduler.check_budgets(url):
            # the budget is debited here, so it has to be last
            reason = 'rejected by crawl budgets'

        if 'skip_crawled' in ridealong:
            self.log_frontier(url)
        elif not self.datalayer.crawled(url):
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

        LOGGER.debug('actually adding url %s, surt %s, for task domain: %s', url.url, url.surt, ridealong['task'].req.url.hostname)
        stats.stats_sum('added urls', 1)

        ridealong['priority'] = priority

        # to randomize fetches, and sub-prioritize embeds
        if ridealong.get('embed'):
            rand = 0.0
        else:
            rand = random.uniform(0, 0.99999)

        #self.scheduler.set_ridealong(url.surt, ridealong)
        #await self.scheduler.queue_work((priority, rand, url.surt))


        self.scheduler.set_ridealong(ridealong['task'].req.url.surt, ridealong)
        await self.scheduler.queue_work((priority, rand, ridealong['task'].req.url.surt))

        self.datalayer.add_crawled(url)
        return 1

    def cancel_workers(self):
        for w in self.workers:
            if not w.done():
                w.cancel()
        # cw = self.control_limit_worker
        # if cw and not cw.done():
        #     cw.cancel()

    def on_finish(self):
        # this is the last method is called before exiting program
        # make external methods calls here, since crawler calls 'exit'
        pass

    async def close(self):
        stats.report()
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

    async def _retry_if_able(self, work, ridealong):
        LOGGER.debug('--> retrying work: {0}'.format(work))
        priority, rand, surt = work
        retries_left = ridealong.get('retries_left', 0) - 1
        if retries_left <= 0:
            # XXX jsonlog hard fail
            # XXX remember that this host had a hard fail
            stats.stats_sum('retries completely exhausted', 1)
            self.scheduler.del_ridealong(surt)

            #seeds.fail(ridealong, self)
            return 'no_retries_left'
        # XXX jsonlog this soft fail
        ridealong['retries_left'] = retries_left
        self.scheduler.set_ridealong(surt, ridealong)
        # increment random so that we don't immediately retry
        extra = random.uniform(0, 0.2)
        priority, rand = self.scheduler.update_priority(priority, rand+extra)
        ridealong['priority'] = priority
        await self.scheduler.requeue_work((priority, rand, surt))
        return ridealong

    async def fetch_and_process(self, work):
        '''
        Fetch and process a single url.
        '''
        priority, rand, surt = work

        # when we're in the dregs of retried urls with high rand, don't exceed priority+1
        stats.stats_set('priority', priority+min(rand, 0.99))

        ridealong = self.scheduler.get_ridealong(surt)
        if 'url' not in ridealong:
            raise ValueError('missing ridealong for surt '+surt)
        url = ridealong['url']
        seed_host = ridealong.get('seed_host', None)

        req_headers, proxy, mock_url, mock_robots = fetcher.apply_url_policies(url, self)

        host_geoip = {}

        if not mock_url:
            entry = await dns.prefetch(url, self.resolver)
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
        if not self.mode == 'cruzer':
            r = await self.robots.check(url, host_geoip, seed_host, self,
                                        headers=req_headers, proxy=proxy, mock_robots=mock_robots)
            if not r:
                # really, we shouldn't retry a robots.txt rule failure
                # but we do want to retry robots.txt failed to fetch
                await self._retry_if_able(work, ridealong)
                return
        # ---> end skip section <--

        _session = self.pool.get_session(ridealong['task'].session_id)

        f = await fetcher.fetch(url, _session, req=ridealong['task'].req, max_page_size=self.max_page_size,
                                    headers=req_headers, proxy=proxy, mock_url=mock_url)



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
                self.scheduler.del_ridealong(surt)

                return ridealong['task']
            else:
                # still has some retries left, res is an old ridealong that was requed
                return res['task']

        self.scheduler.del_ridealong(surt)

        # if f.response.status >= 400 and 'seed' in ridealong:
        #     seeds.fail(ridealong, self)

        json_log['status'] = f.response.status

        if post_fetch.is_redirect(f.response):
            __ridealong = await post_fetch.handle_redirect(f, url, ridealong, priority, host_geoip, json_log, self, seed_host=seed_host)
            # meta-http-equiv-redirect will be dealt with in post_fetch
            if __ridealong and __ridealong == 'no_valid_redir':
                fr_dummy = namedtuple('fr_dummy','response last_exception')
                fr_dummy.response = None # required here
                fr_dummy.last_exception = 'no_valid_redir'
                await self.make_callback(ridealong,fr_dummy)

            else:
                self.add_deffered_task(0,__ridealong)

        else:

            # all redirects already happend, get to callback function

            if f.response.status == 200:

                html, charset_used = await post_fetch.post_200(f, url, priority, host_geoip, seed_host, json_log, self)


                ridealong['task'].doc.html = html

            await self.make_callback(ridealong,f)



        LOGGER.debug('size of work queue now stands at %r urls', self.scheduler.qsize())
        LOGGER.debug('size of ridealong now stands at %r urls', self.scheduler.ridealong_size())
        LOGGER.debug('--> size of deffered queue now stands at %r urls', self.deffered_queue.qsize())

        stats.stats_set('queue size', self.scheduler.qsize())
        stats.stats_max('max queue size', self.scheduler.qsize())

        stats.stats_set('deffered queue size', self.deffered_queue.qsize())
        stats.stats_max('max deffered queue size', self.deffered_queue.qsize())

        stats.stats_set('ridealong size', self.scheduler.ridealong_size())

        if self.crawllogfd:
            print(json.dumps(json_log, sort_keys=True), file=self.crawllogfd)


        return ridealong['task']

    async def make_callback(self,ridealong,f):
        partial = functools.partial(self.load_task_function, ridealong, f)
        with stats.record_burn('--> cruzer callback burn "{0}"'.format(ridealong['task'].name)):
            try:



                handler = CallbackHandler(partial)
                await handler
                #self.loop.run_in_executor(None,partial)

            except ValueError as e:  # if it pukes, ..
                stats.stats_sum('--> parser raised while cruzer callback "{0}"'.format(ridealong['task'].name), 1)
                LOGGER.info('parser raised %r', e)

            except Exception as ex:
                traceback.print_exc()


    def fill_task(self,task,fr):
        task.doc.fetcher = fr
        task.doc.status = fr.response.status if fr.response else fr.last_exception
        task.last_url = str(fr.response.url) if fr.response else None

        return task

    def load_task_function(self,ridealong,fr):
            parent_task = self.fill_task(ridealong['task'],fr)

            task_name = 'task_{0}'.format(parent_task.name)
            task_func = getattr(self,task_name,None)

            if task_func is None:
                raise ValueError('--> Cant find task in Cruzer: {0}'.format(task_name))


            # yeild from function
            task_generator = task_func(parent_task)


            if self.reuse_session:
                self.pool.add_finished_task(parent_task.session_id,parent_task.name)

            try:
                # Attempt to see if you have an iterable object.
                # In the case of a generator or iterator iter simply
                # returns the value it was passed.
                iterator = iter(task_generator)
            except TypeError:
                LOGGER.debug('--> No task left in: {0}, for: {1}'.format(task_name,parent_task.req.url.hostname_without_www))

            except Exception as ex:
                traceback.print_exc()

            else:
                for task in iterator:
                    ride_along = self.get_ridealong(task,parent_task=parent_task)
                    LOGGER.debug('--> New task generated in: {0} -> {1}'.format(task_name,task.name))
                    self.add_deffered_task(0,ride_along)


    async def work(self):
        '''
        Process queue items until we run out.
        '''

        #try:
        while True:

            work = await self.scheduler.get_work()

            try:

                task = await self.fetch_and_process(work)

            except concurrent.futures._base.CancelledError:  # seen with ^C
                pass
            # ValueError('no A records found') should not be a mystery
            except Exception as e:
                # this catches any buggy code that executes in the main thread
                LOGGER.error('Something bad happened working on %s, it\'s a mystery:\n%s', work[2], e)
                traceback.print_exc()
                # falling through causes this work item to get marked done, and we continue

            self.scheduler.work_done()

            if self.stopping:
                raise asyncio.CancelledError

            if self.paused:
                with stats.coroutine_state('paused'):
                    while self.paused:
                        await asyncio.sleep(1)

        # except asyncio.CancelledError:
        #     pass

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

    def memory(self):
        mem = self.scheduler.memory()
        mem.update(self.datalayer.memory())

        if self.reuse_session:
            mem.update(self.pool.memory())

        print('Memory summary')
        for k in sorted(mem.keys()):
            v = mem[k]
            print('  ', k, 'len', v['len'], 'bytes', v['bytes'])
        print('Top objects')
        lines = io.StringIO()
        objgraph.show_most_common_types(limit=20, file=lines)
        lines.seek(0)
        for l in lines.read().splitlines():
            print('  ', l)

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
        if time.time() > self.next_minute:
            self.next_minute = time.time() + 60
            stats.stats_set('DNS cache size', self.resolver.size())
            if self.reuse_session:
                stats.stats_set('Sessions Pool size',self.pool.size())
            stats.report()
            stats.coroutine_report()
            if config.read('Crawl', 'DebugMemory'):
                self.memory()
            if self.reuse_session:
                await self.pool.close_or_wait()


    def update_cpu_stats(self):
        elapsedc = time.clock()  # should be since process start
        stats.stats_set('main thread cpu time', elapsedc)

    def get_ridealong(self,task,parent_task=None):
        # url = Url instance

        if parent_task is not None:
            # parent_task exists only in deffered queue
            task.add_parent(parent_task)
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

        ride_along = {'url': task.req.url,'task':task,'skip_seen_url':True}
        return ride_along

    def task_generator(self):
        yield ':)'

    def add_deffered_task(self,priority, ridealong):
        #priority =  lower number = higher priority
        # add urls from redirects detection and from cruzer callback functions
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
        while True:
            try:
                priority, ridealong = self.deffered_queue.get_nowait()

                await self.add_url(priority,ridealong)
                LOGGER.debug('--> deffered task added, actual url: {0} , task domain: {1}'.format(ridealong['url'].url,
                                                                                               ridealong['task'].req.url.hostname))
                self.deffered_queue.task_done() # this wont be reached if the queue is empty
            except asyncio.queues.QueueEmpty:
                LOGGER.debug('--> deffered queue is empty')
                await asyncio.sleep(0.5)

            except concurrent.futures._base.CancelledError:  # seen with ^C
                pass
            except Exception as ex:
                traceback.print_exc()
                break

            if self.stopping:

                raise asyncio.CancelledError



    async def queue_producer(self):
        while True:
            # deffered queue has priority over initail urls, that why we also include it here
            # do not try to get items from this queue here since it could be racy with main coroutine
            if not self.deffered_queue.empty():
                await asyncio.sleep(0.1)
            try:
                task = next(self.init_generator)
                LOGGER.debug('--> new task submited: "{0}" for {1}'.format(task.name,task.req.url.url))
            except StopIteration:
                LOGGER.debug('--> cruzer iter is empty')
                break

            except Exception as ex:
                traceback.print_exc()
                break

            ride_along = self.get_ridealong(task)

            await self.add_url(1,ride_along)

            if self.stopping:
                raise asyncio.CancelledError




    async def crawl(self):
        '''
        Run the crawler until it's out of work
        '''
        self.producer = asyncio.Task(self.queue_producer())

        #self.control_limit_worker = asyncio.Task(self.control_limit())

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

                await self.pool.close_or_wait()

                if not self.deffered_queue.empty():
                    # there is no point calling join on this queue since it's marked as complete once items is taken
                    LOGGER.warning('--> queue is about to join, but we still have things in deffered queue, waiting')
                    await asyncio.sleep(1)

                elif self.pool.busy == True:
                    LOGGER.warning('--> queue is about to join, but there are tasks in submited list, [ main queue: {0}'
                                   ' , deffered queue: {1} , ridealong size: {2} ]'.format(self.scheduler.qsize(),self.deffered_queue.qsize(),len(self.scheduler.ridealong)))
                    await asyncio.sleep(1)

                else:
                    LOGGER.warning('all workers appear idle, queue appears empty, executing join')

                    await self.scheduler.close()
                    await self.deffered_queue.join()
                    break

            self.update_cpu_stats()
            # show stats and close finished sessions
            await self.minute()



        if config.read('Crawl', 'DebugMemory'):
            self.memory()

        self.cancel_workers()
        self.producer.cancel()
        self.deffered_queue_checker.cancel()



        if self.stopping or config.read('Save', 'SaveAtExit'):
            self.summarize()
            self.datalayer.summarize()
            LOGGER.warning('saving datalayer and queues')
            self.save_all()
            LOGGER.warning('saving done')

    @classmethod
    def limit_resources(cls):
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        # XXX warn if too few compared to max_wokers?
        if sys.platform=='darwin':
            hard = 10240

        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

        _, hard = resource.getrlimit(resource.RLIMIT_AS)  # RLIMIT_VMEM does not exist?!
        rlimit_as = int(config.read('System', 'RLIMIT_AS_gigabytes'))
        rlimit_as *= 1024 * 1024 * 1024
        if rlimit_as == 0:
            return
        if hard > 0 and rlimit_as > hard:
            LOGGER.error('RLIMIT_AS limited to %d bytes by system limit', hard)
            rlimit_as = hard
        resource.setrlimit(resource.RLIMIT_AS, (rlimit_as, hard))


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

        config.config(args.configfile, args.config, confighome=not args.no_confighome)

        cls.limit_resources()

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

        loop = asyncio.get_event_loop()

        #loop.set_debug(True)

        slow_callback_duration = os.getenv('ASYNCIO_SLOW_CALLBACK_DURATION')
        if slow_callback_duration:
            loop.slow_callback_duration = float(slow_callback_duration)
            LOGGER.warning('set slow_callback_duration to %f', slow_callback_duration)

        if config.read('CarbonStats'):
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
            if config.read('CarbonStats'):
                timer.close()
            # apparently this is needed for full aiohttp cleanup -- or is it cargo cult
            loop.stop()
            loop.run_forever()
            loop.close()
            cruzer.on_finish()


        exit(stats.exitstatus)
