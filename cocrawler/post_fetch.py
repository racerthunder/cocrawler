'''
builtin post_fetch event handler
special seed handling -- don't count redirs in depth, and add www.seed if naked seed fails (or reverse)
do this by elaborating the work unit to have an arbitrary callback

parse links and embeds, using an algorithm chosen in conf -- cocrawler.cocrawler

parent subsequently calls add_url on them -- cocrawler.cocrawler
'''

import logging
from functools import partial
import json
import codecs
import traceback

import multidict
from bs4 import BeautifulSoup

from . import urls
from . import parse
from . import stats
from . import config
from . import seeds
from . import facet
from . import geoip
from . import content

LOGGER = logging.getLogger(__name__)


# aiohttp.ClientReponse lacks this method, so...
def is_redirect(response):
    return 'Location' in response.headers and response.status in (301, 302, 303, 307, 308)


def charset_log(json_log, charset, detect, charset_used):
    '''
    Log details, but only if interesting
    '''
    interesting = False

    if ' replace' in charset_used:
        interesting = True
    elif not charset:
        interesting = True
        stats.stats_sum('cchardet used', 1)
    elif charset != charset_used:
        interesting = True
        stats.stats_sum('cchardet used', 1)

    if interesting:
        json_log['cchardet_charset'] = detect['encoding']
        json_log['cchardet_confidence'] = detect['confidence']

    json_log['charset'] = charset_used
    stats.stats_sum('charset='+charset_used, 1)


def minimal_facet_me(resp_headers, url, host_geoip, kind, t, crawler, seed_host=None, location=None):
    if not crawler.facetlogfd:
        return

    head_soup = BeautifulSoup('', 'lxml')
    facets = facet.compute_all('', '', '', resp_headers, [], [], head_soup=head_soup, url=url)
    geoip.add_facets(facets, host_geoip)
    if not isinstance(url, str):
        url = url.url

    facet_log = {'url': url, 'facets': facets, 'kind': kind, 'time': t}
    if seed_host:
        facet_log['seed_host'] = seed_host
    if location:  # redirect
        facet_log['location'] = location

    print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)


'''
If we're robots blocked, the only 200 we're ever going to get is
for robots.txt. So, facet it.
'''


def post_robots_txt(f, url, host_geoip, t, crawler, seed_host=None):
    resp_headers = f.response.headers
    minimal_facet_me(resp_headers, url, host_geoip, 'robots.txt', t, crawler, seed_host=seed_host)


'''
Study redirs at the host level to see if we're systematically getting
redirs from bare hostname to www or http to https, so we can do that
transformation in advance of the fetch.

Try to discover things that look like unknown url shorteners. Known
url shorteners should be treated as high-priority so that we can
capture the real underlying url before it has time to change, or for
the url shortener to go out of business.
'''


async def handle_redirect(f, url, ridealong, priority, host_geoip, json_log, crawler, rand=None):
    resp_headers = f.response.headers
    seed_host = ridealong.get('seed_host')

    location = resp_headers.get('location')
    if location is None:
        seeds.fail(ridealong, crawler)
        LOGGER.debug('%d redirect for %s has no Location: header', f.response.status, url.url)
        raise ValueError(url.url + ' sent a redirect with no Location: header')
    next_url = urls.URL(location, urljoin=url)

    minimal_facet_me(resp_headers, url, host_geoip, 'redir', json_log['time'], crawler,
                     seed_host=seed_host, location=next_url.url)

    ridealong['task'].req.url = next_url

    redir_kind = urls.special_redirect(url, next_url)
    samesurt = url.surt == next_url.surt

    if 'seed' in ridealong:
        prefix = 'redirect seed'
    else:
        prefix = 'redirect'
    if redir_kind is not None:
        stats.stats_sum(prefix+' '+redir_kind, 1)
    else:
        stats.stats_sum(prefix+' non-special', 1)

    queue_next = True

    if redir_kind is None:
        if samesurt:
            LOGGER.debug('Whoops, %s is samesurt but not a special_redirect: %s to %s, location %s',
                        prefix, url.url, next_url.url, location)
    elif redir_kind == 'same':
        LOGGER.debug('attempted redirect to myself: %s to %s, location was %s', url.url, next_url.url, location)
        if 'Set-Cookie' not in resp_headers:
            LOGGER.debug(prefix+' to myself and had no cookies.')
            stats.stats_sum(prefix+' same with set-cookie', 1)
        else:
            stats.stats_sum(prefix+' same without set-cookie', 1)
        seeds.fail(ridealong, crawler)
        queue_next = False

    else:
        LOGGER.debug('special redirect of type %s for url %s', redir_kind, url.url)
        # XXX push this info onto a last-k for the host
        # to be used pre-fetch to mutate urls we think will redir

    if config.read('Crawl', 'AllowExternalRedir') is False:
        # do not allow redirect to external domains, consequently www. or https to the same domain is left as valid
        if url.hostname_without_www != next_url.hostname_without_www:
            LOGGER.debug('--> No external redirects allowed: {0} to {1}'.format(url.url, next_url.url))
            queue_next = False

    priority += 1

    if samesurt and redir_kind != 'same':
        ridealong['skip_crawled'] = True

    if 'freeredirs' in ridealong:

        priority -= 1
        json_log['freeredirs'] = ridealong['freeredirs']
        ridealong['freeredirs'] -= 1
        if ridealong['freeredirs'] == 0:
            del ridealong['freeredirs']
    ridealong['priority'] = priority

    json_log['redirect'] = next_url.url
    json_log['location'] = location
    if redir_kind is not None:
        json_log['redir_kind'] = redir_kind
    if queue_next:
        json_log['found_new_links'] = 1
    else:
        json_log['found_new_links'] = 0

    default_error = 'no_valid_redir'

    if 'redirs_left' in ridealong:
        ridealong['redirs_left'] -= 1
        if ridealong['redirs_left'] == 0:
            queue_next = False
            default_error = 'max_redirs_reached'

    # after we return, json_log will get logged
    if queue_next:
        return ridealong
    else:
        return default_error

async def post_200(f, url, ridealong, priority, host_geoip, json_log, crawler, run_bunner=False):


    if crawler.warcwriter is not None:
        # needs to use the same algo as post_dns for choosing what to warc
        # insert the digest instead of computing it twice? see sha1 below
        # we delayed decompression so that we could warc the compressed body
        crawler.warcwriter.write_request_response_pair(url.url, f.req_headers,
                                                       f.response.raw_headers, f.is_truncated, f.body_bytes,
                                                       decompressed=False)

    resp_headers = f.response.headers
    content_type, content_encoding, charset = content.parse_headers(resp_headers, json_log)


    html_types = set(('text/html', 'text/css', '', 'application/xhtml+xml','application/json'))

    if content_type in html_types:
        if content_encoding != 'identity':
            with stats.record_burn('response body decompress', url=url):
                body_bytes = content.decompress(f.body_bytes, content_encoding, url=url)
            stats.stats_sum('response body decompress bytes', len(body_bytes))
        else:
            body_bytes = f.body_bytes

        with stats.record_burn('response body get_charset', url=url):
            charset, detect = content.my_get_charset(charset, body_bytes)
        with stats.record_burn('response body decode', url=url):
            body, charset_used = content.my_decode(body_bytes, charset, detect)

        charset_log(json_log, charset, detect, charset_used)

        content_data = (content_type, content_encoding, charset, charset_used) # attached to task.doc
        if run_bunner:
            try:
                result = await do_parser(body, body_bytes, resp_headers, url, crawler)
                return body, content_data, result
            except ValueError as e:
                stats.stats_sum('parser raised', 1)
                LOGGER.info('parser raised %r', e)
                # XXX jsonlog
                return None, None, None


        return body, content_data, None

    else:
        return None, None, None

    # left to make merge easier
    try:
        links, embeds, sha1, facets, base = await do_parser(body, body_bytes, resp_headers, url, crawler)
    except ValueError as e:
        stats.stats_sum('parser raised', 1)
        LOGGER.info('parser raised %r', e)
        # XXX jsonlog
        return

    json_log['checksum'] = sha1

    geoip.add_facets(facets, host_geoip)

    facet_log = {'url': url.url, 'facets': facets, 'kind': 'get'}
    if base is not None:
        facet_log['base'] = base
    facet_log['checksum'] = sha1
    facet_log['time'] = json_log['time']

    seed_host = ridealong.get('seed_host')
    if seed_host:
        facet_log['seed_host'] = seed_host

    if crawler.facetlogfd:
        print(json.dumps(facet_log, sort_keys=True), file=crawler.facetlogfd)

    LOGGER.debug('parsing content of url %r returned %d links, %d embeds, %d facets',
                 url.url, len(links), len(embeds), len(facets))
    json_log['found_links'] = len(links) + len(embeds)
    stats.stats_max('max urls found on a page', len(links) + len(embeds))

    max_tries = config.read('Crawl', 'MaxTries')
    queue_embeds = config.read('Crawl', 'QueueEmbeds')

    new_links = 0
    ridealong_skeleton = {'priority': priority+1, 'retries_left': max_tries}
    if seed_host:
        ridealong_skeleton['seed_host'] = seed_host
    for u in links:
        ridealong = {'url': u}
        ridealong.update(ridealong_skeleton)
        if crawler.add_url(priority + 1, ridealong):
            new_links += 1
    if queue_embeds:
        for u in embeds:
            ridealong = {'url': u}
            ridealong.update(ridealong_skeleton)
            if crawler.add_url(priority - 1, ridealong):
                new_links += 1

    if new_links:
        json_log['found_new_links'] = new_links

    # XXX process meta-http-equiv-refresh

    # XXX plugin for links and new links - post to Kafka, etc
    # neah stick that in add_url!

    # actual jsonlog is emitted after the return


async def do_parser(body, body_bytes, resp_headers, url, crawler):
    if len(body) > int(config.read('Multiprocess', 'ParseInBurnerSize')):
        stats.stats_sum('parser in burner thread', 1)
        # headers is a multidict.CIMultiDictProxy case-blind dict
        # and the Proxy form of it doesn't pickle, so convert to one that does
        resp_headers = multidict.CIMultiDict(resp_headers)
        links, embeds, sha1, facets, base = await crawler.burner.burn(
            partial(parse.do_burner_work_html, body, body_bytes, resp_headers,
                    burn_prefix='burner ', url=url),
            url=url)
    else:
        stats.stats_sum('parser in main thread', 1)
        # no coroutine state because this is a burn, not an await
        links, embeds, sha1, facets, base = parse.do_burner_work_html(
            body, body_bytes, resp_headers, burn_prefix='main ', url=url)

    return links, embeds, sha1, facets, base


def post_dns(dns, expires, url, crawler):
    if crawler.warcwriter is not None:  # needs to use the same algo as post_200 for choosing what to warc
        crawler.warcwriter.write_dns(dns, expires, url)
