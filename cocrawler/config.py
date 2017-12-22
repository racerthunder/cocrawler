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
  FreeSeedRedirs: 3
  SeedRetries: 4

REST: {}
#REST:
#  ServerIP: 0.0.0.0
#  ServerPort: 8080

Crawl:
  MaxDepth: 3
  MaxTries: 4
  PageTimeout: 30
  RetryTimeout: 5
  MaxWorkers: 10
  MaxHostQPS: 10
#  MaxCrawledUrls: 11
#  CookieJar: Defective

UserAgent:
  Style: laptopplus
  MyPrefix: test
  URL: http://cocrawler.com/cocrawler.html

Robots:
  MaxTries: 4
  RobotsCacheSize: 1000
  RobotsCacheTimeout: 86400

Fetcher:
  Nameservers:
  - 8.8.8.8
  - 8.8.4.4
  CrawlLocalhost: False  # crawl ips that resolve to localhost
  CrawlPrivate: False  # crawl ips that resolve to private networks (e.g. 10.*/8)
  DNSCacheMaxSize: 1000000

#CarbonStats:
#  Server: localhost
#  Port: 2004

Plugins:
  url_allowed: SeedsHostname

Multiprocess:
  BurnerThreads: 2
  ParseInBurnerSize: 20000
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

Logging: {}
#Logging:
# note that the following files are all opened for append, for
# restart purposes. Might change to unique filenames?
#  Crawllog: crawllog.jsonl
#  Robotslog: robotslog.jsonl
#  RejectedAddUrllog: rejectedaddurl.log
#  Facetlog: facet.log

Testing:
  TestHostmapAll: False
#  TestHostmapAll: test.website: localhost:8080
#  StatsEQ:
#    fetch http code=200: 1000
#    fetch URLs: 1000
#    max urls found on a page: 3

'''


def print_default():
    print(default_yaml)


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
                temp[key] = rhs
            except KeyError:
                LOGGER.error('invalid config of %s', c)
                continue

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
