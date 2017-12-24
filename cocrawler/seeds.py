import urllib
import logging

from . import config
from . import stats
from .urls import URL
from . import url_allowed

LOGGER = logging.getLogger(__name__)

POLICY = None
valid_policies = set(('None', 'www-then-non-www'))


def expand_seeds_config(crawler):
    urls = []
    seeds = config.read('Seeds')

    if seeds is None:
        return

    global POLICY
    POLICY = config.read('Seeds', 'Policy')
    if POLICY not in valid_policies:
        raise ValueError('config Seeds Policy is not valid: '+POLICY)

    if seeds.get('Hosts', []):
        for h in seeds['Hosts']:
            u = special_seed_handling(h)
            urls.append(u)

    seed_files = seeds.get('Files', [])
    if seed_files:
        if not isinstance(seed_files, list):
            seed_files = [seed_files]
        for name in seed_files:
            LOGGER.info('Loading seeds from file %s', name)
            with open(name, 'r') as f:
                for line in f:
                    if '#' in line:
                        line, _ = line.split('#', 1)
                    if line.strip() == '':
                        continue
                    u = special_seed_handling(line.strip())
                    urls.append(u)

    # sitemaps are a little tedious, so I'll implement later.
    # needs to be fetched and then xml parsed and then <urlset ><url><loc></loc> elements extracted

    return seed_some_urls(urls, crawler)


def seed_some_urls(urls, crawler):
    freeseedredirs = config.read('Seeds', 'FreeSeedRedirs')
    retries_left = config.read('Seeds', 'SeedRetries') or config.read('Crawl', 'MaxTries')
    priority = 1

    url_allowed.setup_seeds(urls)

    for u in urls:
        ridealong = {'url': u, 'priority': priority, 'seed': True,
                'skip_seen_url': True, 'retries_left': retries_left}
        if freeseedredirs:
            ridealong['free_redirs'] = freeseedredirs
        crawler.add_url(priority, ridealong)

    stats.stats_sum('seeds added', len(urls))
    return urls


def special_seed_handling(url):
    '''
    We don't expect seed-lists to be very clean: no scheme, etc.
    '''
    parts = urllib.parse.urlsplit(url)
    if parts.scheme == '':
        if url.startswith('//'):
            url = 'http:' + url
        else:
            url = 'http://' + url

    if POLICY == 'www-then-non-www':
        # does hostname already have www? use URL() to find out
        temp = URL(url)
        if temp.hostname == temp.hostname_without_www:
            url.replace('http://', 'http://www.')

    url = URL(url)
    return url


def fail(ridealong, crawler):
    '''
    Called for all final failures
    '''
    if 'seed' not in ridealong:
        return

    url = ridealong['url']
    LOGGER.info('Received a final failure for seed url %s', url.url)
    stats.stats_sum('seeds failed', 1)

    if POLICY == 'www-then-non-www':
        # OK so url could have changed because of a redirect.
        # I'm too lazy to fix this the right way, so let's do it wrong.
        if url.hostname != url.hostname_without_www:
            url = URL('http://' + url.netloc.replace('www.', '', 1) + '/')
            LOGGER.info('seed url second chance %s', url.url)
            stats.stats_sum('seeds second chances', 1)
            seed_some_urls((url,), crawler)
