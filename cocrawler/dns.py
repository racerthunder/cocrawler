'''
DNS-related code
'''

import time
import logging
import urllib
import ipaddress
import collections.abc

import cachetools
import aiohttp
import aiodns
import pympler

from . import stats
from . import config
from pathlib import Path
from . import memory

LOGGER = logging.getLogger(__name__)


async def prefetch(url, resolver):
    with stats.coroutine_state('DNS prefetch'):
        with stats.record_latency('DNS prefetch', url=url.hostname):
            try:
                await resolver.resolve(url.hostname, 80, stats_prefix='prefetch ')
            except OSError:  # mapped to aiodns.error.DNSError if it was a .get
                stats.stats_sum('prefetch DNS error', 1)
                return None

            except UnicodeError as e:
                stats.stats_sum('prefetch DNS unicode error', 1)
                LOGGER.info('UnicodeError prefetching dns for %s: %s', url.hostname, str(e))
                return None
            except ValueError:
                stats.stats_sum('prefetch DNS no A records found', 1)
                return None

    return resolver.get_cache_entry(url.hostname)


class CoCrawler_Caching_AsyncResolver(aiohttp.resolver.AsyncResolver):
    '''
    A caching dns wrapper that lets us subvert aiohttp's built-in dns policies

    Use a LRU cache which respects TTL and is bounded in size.
    Refetch dns (once!) when the TTL is 3/4ths expired.

    TODO: Warc the answer
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._crawllocalhost = config.read('Fetcher', 'CrawlLocalhost') or False
        self._crawlprivate = config.read('Fetcher', 'CrawlPrivate') or False
        self._cachemaxsize = config.read('Fetcher', 'DNSCacheMaxSize')
        self._cache = cachetools.LRUCache(int(self._cachemaxsize))
        self._refresh_in_progress = set()

        memory.register_debug(self.memory)

    async def resolve(self, host, port=0, stats_prefix='fetch ', **kwargs):
        t = time.time()
        if host in self._cache:
            stats.stats_sum(stats_prefix+'DNS cache hit', 1)
            addrs, expires, refresh, host_geoip = self._cache[host]
            if expires < t:
                stats.stats_sum(stats_prefix+'DNS cache hit expired entry', 1)
                del self._cache[host]
                expire_some(t, self._cache, 100, stats_prefix=stats_prefix)
            elif refresh < t and host not in self._refresh_in_progress:
                stats.stats_sum(stats_prefix+'DNS cache hit entry refresh', 1)
                # TODO: spawn a thread to await this while I continue on
                self._refresh_in_progress.add(host)
                stats.stats_sum(stats_prefix+'DNS refresh lookup', 1)
                stats.stats_sum('DNS external queries', 1)
                self._cache[host] = await self.actual_async_lookup(host, port=port, **kwargs)
                self._refresh_in_progress.remove(host)

        if host not in self._cache:
            stats.stats_sum(stats_prefix+'DNS lookup after cache miss begun', 1)
            stats.stats_sum('DNS external queries', 1)
            self._cache[host] = await self.actual_async_lookup(host, port=port, **kwargs)
            # no A's is a ValueError so we do not cache them
            stats.stats_sum(stats_prefix+'DNS lookup after cache miss success', 1)

        addrs = self._cache[host][0]
        # if the cached entry was made with a different port, lie about it
        for a in addrs:
            if 'port' in a:
                a['port'] = port
        return addrs

    async def actual_async_lookup(self, host, port=0, **kwargs):
        '''
        Do an actual lookup. Always raise if it fails.
        '''
        # can raise OSError: Domain name not found
        # (which ends up being ClientConnectError if inside an aiohttp .get)
        addrs = await super().resolve(host, port=port, **kwargs)

        # filter return value to exclude unwanted ip addrs
        ret = []
        ttl = 0
        for a in addrs:
            if 'host' not in a:
                continue
            try:
                ip = ipaddress.ip_address(a['host'])
            except ValueError:
                continue
            if not self._crawllocalhost and ip.is_loopback:
                stats.stats_sum('DNS filter removed loopback', 1)
                continue
            if not self._crawlprivate and ip.is_private:
                stats.stats_sum('DNS filter removed private', 1)
                continue
            if ip.is_multicast:
                stats.stats_sum('DNS filter removed multicast', 1)
                continue
            ret.append(a)
            if 'ttl' in a:
                ttl = a['ttl']  # all should be equal, we'll remember the last

        if len(addrs) != len(ret):
            LOGGER.debug('threw out some ip addresses for %s', host)
        if len(ret) == 0:
            stats.stats_sum('DNS lookup no A records found', 1)
            raise ValueError('no A records found')

        ttl = max(3600*8, min(3600, ttl))  # force ttl into a range of time
        t = time.time()
        expires = t + ttl
        refresh = t + (ttl * 0.75)
        host_geoip = {}

        return ret, expires, refresh, host_geoip

    def get_cache_entry(self, host):
        if host in self._cache:
            return self._cache[host]

    def size(self):
        return len(self._cache)

    def memory(self):
        resolver_cache = {}
        resolver_cache['bytes'] = memory.total_size(self._cache)
        resolver_cache['len'] = len(self._cache)
        return {'resolver_cache': resolver_cache}


def expire_some(t, lru, some, stats_prefix=''):
    # examine a few of the oldest entries to see if they're expired
    # this keeps deadwood from building up in the cache
    for i in range(some):
        host, entry = lru.popitem()
        if entry[1] > t:
            LOGGER.debug('expire_some expired %d entries', i)
            stats.stats_sum(stats_prefix+'DNS cache expire_some', i)
            lru[host] = entry
            break

def get_resolver(ns=None):

    if ns is None:
        raise ValueError('--> Ns should not be empty')

    ns_tries = config.read('Fetcher', 'NameserverTries')
    ns_timeout = config.read('Fetcher', 'NameserverTimeout')

    return CoCrawler_Caching_AsyncResolver(nameservers=ns, tries=ns_tries,
                                           timeout=ns_timeout, rotate=True)


def entry_to_ip_key(entry):
    if entry is None:
        return
    addrs = entry[0]
    return ','.join(sorted([a['host'] for a in addrs]))

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
