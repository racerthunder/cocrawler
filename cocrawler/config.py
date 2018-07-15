import os
import collections.abc
import logging
import yaml

LOGGER = logging.getLogger(__name__)

__gobal_config = None

'''
default_yaml exists to both set defaults and to document all
possible configuration variables.
'''

default_yaml = '''
PluginPath: False
Plugins:
- plugins/default-plugins.cfg

Seeds:
#  Hosts:
#  - http://xkcd.com/
#  Files:
#  - seed_list.txt
#  CrawledFiles:
#  - crawled_list.txt
  FreeSeedRedirs: 3  # many seeds do lots of redirs
  SeedRetries: 4  # overwrites the standard crawl retry
#  Policy: www-then-non-www
  Policy: None

REST: {}
#REST:
#  ServerIP: localhost  # or 0.0.0.0, if you dare
#  ServerPort: 8080  # add a + to search for a port

Crawl:
  MaxDepth: 3
  MaxTries: 2
  PageTimeout: 5
  RetryTimeout: 5
  MaxWorkers: 10
  MaxHostQPS: 10
  MaxPageSize: 1000000
  PreventCompression: False
  UpgradeInsecureRequests: 1
  ConnectTimeout: 2  # seconds, 0.=none
#  GlobalBudget: None
#  DomainBudget: None
#  HostBudget: None

UserAgent:
  Style: crawler
  MyPrefix: test-deep
  URL: http://example.com/cocrawler.html
  UA: Googlebot/2.1 (+http://www.google.com/bot.html)
Robots:
  MaxTries: 4
  RobotsCacheSize: 1000
  RobotsCacheTimeout: 86400
  MaxRobotsPageSize: 500000

Fetcher:
  Nameservers:
   File:
   - dns.txt
  NameserverTries: 10
  NameserverTimeout: 3.0
  CrawlLocalhost: False  # crawl ips that resolve to localhost
  CrawlPrivate: False  # crawl ips that resolve to private networks (e.g. 10.*/8)
  DNSCacheMaxSize: 1000000

GeoIP:
  DataDir: None

#CarbonStats:
#  Server: localhost
#  Port: 2004

Plugins:
  url_allowed: AllDomains

Multiprocess:
  ParseInBurnerSize: 1
  BurnerThreads: 2
#  ParseInBurnerSize: 20000
#  Affinity: yes

Save:
#   Name:
#   SaveAtExit:
   Overwrite: False

WARC:
  WARCAll: False
  WARCMaxSize: 1000000000
  WARCPrefix: Testing
  WARCDescription: A WARC generated by CoCrawler's automated tests

#Logging: {}
Logging:
# note that the following files are all opened for append, for
# restart purposes. Might change to unique filenames?
#  Crawllog: ../log/crawllog.jsonl
#  Frontierlog: frontierlog
#  Robotslog: robotslog.jsonl
   RejectedAddUrllog: ../log/rejectedaddurl.log
#  Facetlog: facet.log

Testing:
  TestHostmapAll: False
#  TestHostmapAll: test.website: localhost:8080
#  StatsEQ:
#    fetch http code=200: 1000
#    fetch URLs: 1000
#    max urls found on a page: 3

System:
  RLIMIT_AS_gigabytes: 0  # 0=do not set

'''


def print_default():
    print(default_yaml)


def print_final():
    print(repr(__global_config))


def merge_dicts(a, b):
    '''
    Merge 2-level dict b into a, b overwriting a if present.
    Not very general purpose!
    XXX does having c here actually do anything? is c=a a
    '''
    c = a
    for k1 in b:
        for k2 in b[k1]:
            v = b[k1][k2]
            if k1 not in c or not c[k1]:
                c[k1] = {}
            if k2 not in c[k1]:
                c[k1][k2] = {}
            c[k1][k2] = v
    return c


def config(configfile, configlist, confighome=True):
    '''
    Return a config dict which is the sum of all the various configurations
    '''

    default = yaml.safe_load(default_yaml)

    config_from_file = {}
    if configfile:
        LOGGER.info('loading %s', configfile)
        try:
            with open(configfile, 'r') as c:
                config_from_file = yaml.safe_load(c)
        except FileNotFoundError:
            LOGGER.error('configfile %s not found', configfile)
            exit(1)

    combined = merge_dicts(default, config_from_file)

    homefile = os.path.expanduser('~/.cocrawler-config.yml')
    if confighome and os.path.exists(homefile):
        LOGGER.info('loading ~/.cocrawler-config.yml')
        with open(homefile, 'r') as c:
            config_from_file = yaml.safe_load(c)
        combined = merge_dicts(combined, config_from_file)
    elif confighome:
        LOGGER.info('~/.cocrawler-config.yml not found')

    if configlist:
        for c in configlist:
            # the syntax is... dangerous
            if ':' not in c:
                LOGGER.error('invalid config of %s', c)
                continue
            lhs, rhs = c.split(':', maxsplit=1)
            if '.' not in lhs:
                LOGGER.error('invalid config of %s', c)
                continue
            xpath = lhs.split('.')
            key = xpath.pop()
            try:
                temp = combined
                for x in xpath:
                    temp = combined[x]
            except KeyError:
                LOGGER.error('invalid config of %s', c)
                continue
            else:
                # What type should this be? Maybe not a string.
                temp[key] = type_fixup(rhs)

    global __global_config
    __global_config = combined


def read(*l):
    if not isinstance(l, collections.abc.Sequence):
        l = (l,)
    c = __global_config
    for name in l:
        if c is None:
            LOGGER.error('invalid config key %r', l)
            raise ValueError('invalid config key')
        c = c.get(name)
    return c


def write(value, *l):
    if not isinstance(l, collections.abc.Sequence):
        l = (l,)
    l = list(l)  # so I can pop it
    last = l.pop()
    c = __global_config
    for name in l:
        if c is None:
            LOGGER.error('invalid config key %r', l)
            raise ValueError('invalid config key')
        c = c.get(name)

    if not isinstance(c, collections.abc.MutableMapping):
        LOGGER.error('invalid config key %r', l)
        raise ValueError('invalid config key')

    c[last] = value


def set_config(c):
    '''
    Used in unit tests
    '''
    global __global_config
    __global_config = c


def type_fixup(rhs):
    if rhs.startswith('[') and rhs.endswith(']'):
        return rhs[1:len(rhs)-1].split(',')
    return rhs
