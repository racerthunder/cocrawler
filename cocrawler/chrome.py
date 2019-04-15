import time
import traceback
from collections import namedtuple
import ssl
import urllib
import json
from pprint import pformat
from random import randint
from pyppeteer import launch

import asyncio
import logging
import aiohttp
import async_timeout


from . import stats
from . import config
from . import content
from . resp import Resp
from . fetcher import FetcherResponse

LOGGER = logging.getLogger(__name__)

async def fetch(url, session, req=None, headers=None, proxy=None, mock_url=None,dns_entry=None,
                allow_redirects=None, max_redirects=None,
                stats_prefix='', max_page_size=-1):

    t0 = time.time()
    last_exception = None
    body_bytes = b''
    blocks = []
    left = max_page_size
    ip = None

    if req.method is 'POST':
        raise ValueError('--> Post is not supported by chrome mode')

    # Dead straight timeout to fight hanged mostly https connections in aiohttp pool
    with async_timeout.timeout(int(config.read('Crawl', 'PageTimeout'))):
        with stats.coroutine_state(stats_prefix+'fetcher fetching'):
            with stats.record_latency(stats_prefix+'fetcher fetching', url=url.url):

                page = await session.newPage()

                if req.chrome_cookies is not None:
                    await page.setCookie(*req.chrome_cookies)


                t_first_byte = '{:.3f}'.format(time.time() - t0)

                response = await page.goto(url.url)

                #html =  await page.content()

                body_bytes = await response.buffer()

                if isinstance(body_bytes, (str,)):
                    body_bytes = bytes(body_bytes, encoding='utf-8')

                await asyncio.sleep(1)
                await page.close()

                t_last_byte = '{:.3f}'.format(time.time() - t0)



    if last_exception is not None:

        LOGGER.info('CHROME failed working on %s, the last exception is %s', url.url, last_exception)
        return FetcherResponse(None, None, None, None, None, None, False, last_exception)

        # create new class response not to bring entire asyncio Response class along the workflow (memory leak)
    resp = Resp(url=response.url,
                status = response.status,
                headers=response.headers,
                raw_headers = response.headers)

    fr = FetcherResponse(resp, body_bytes, ip, response.headers, t_first_byte, t_last_byte, False, None)

    if resp.status >= 500:
        LOGGER.debug('server returned http status %d', resp.status)

    stats.stats_sum(stats_prefix+'fetch bytes', len(body_bytes) + len(response.headers))

    stats.stats_sum(stats_prefix+'fetch URLs', 1)
    stats.stats_sum(stats_prefix+'fetch http code=' + str(resp.status), 1)

    # checks after fetch:
    # hsts header?
    # if ssl, check strict-transport-security header, remember max-age=foo part., other stuff like includeSubDomains
    # did we receive cookies? was the security bit set?

    log_headers = None #pformat(dict(response.raw_headers),indent=10)
    dns_line = None #'dns [{0}]: {1}'.format(dns_log[0],dns_log[1])

    LOGGER.debug('<{0} [{1}] {2}  {3} > \n {4}'.format(
        req.method,
        resp.status,
        url.url,
        dns_line or '',
        log_headers or ''
    ))

    return fr
