#!/usr/bin/env python

'''
CoCrawler web crawler, main program
'''
import sys
import resource
import os
import faulthandler
import gc

import argparse
import asyncio
import logging
import warnings

import cocrawler
import cocrawler.config as config
import cocrawler.stats as stats
import cocrawler.timer as timer
import cocrawler.webserver as webserver
from cocrawler.urls import URL

LOGGER = logging.getLogger(__name__)

faulthandler.enable()

ARGS = argparse.ArgumentParser(description='CoCrawler web crawler')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store', default='/Volumes/crypt/_Coding/PYTHON/cocrawler/tests/test-brc.yml')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--loglevel', action='store', default='DEBUG')
ARGS.add_argument('--load', action='store')


def limit_resources():
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

class Task(object):
    def __init__(self,name,url=None,**kwargs):
        assert url is not None
        self.url = URL(url)
        self.name = name

        if kwargs:
            for k,v in kwargs.items():
                setattr(self,k,v)


def dispatcher():
    urls = ['http://tut.by','http://mail.ru','http://habr.com']

    for url in urls:
        yield url
        #break

class Cruzer(cocrawler.Crawler):

    def task_generator(self):
        for url in dispatcher():
            yield Task(name='download',url=url,id=1)

    def task_download(self,task,fr,):
        print('--> calling function download, task id = {0}, url={1}'.format(task.id,fr.response.url))
        yield Task(name='second',url='http://google.com?ref={0}'.format(fr.response.url),id=2)

    def task_second(self,task,fr):
        print('--> calling task_second, task id = {0}, url={1}'.format(task.id,fr.response.url))
        #print(fr.response)




def main():
    '''
    Main program: parse args, read config, set up event loop, run the crawler.
    '''

    args = ARGS.parse_args()

    if args.printdefault:
        config.print_default()
        sys.exit(1)

    loglevel = os.getenv('COCRAWLER_LOGLEVEL') or args.loglevel
    logging.basicConfig(level=loglevel)

    config.config(args.configfile, args.config, confighome=not args.no_confighome)

    limit_resources()

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

    cruzer = Cruzer(**kwargs)

    loop = asyncio.get_event_loop()
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


if __name__ == '__main__':
    main()
    exit(stats.exitstatus)
