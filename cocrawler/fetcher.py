'''
async fetching of urls.

Assumes robots checks have already been done.

Supports server mocking; proxies are not yet implemented.

Success returns response object and response bytes (which were already
read in order to shake out all potential network-related exceptions.)

Failure returns enough details for the caller to do something smart:
503, other 5xx, DNS fail, connect timeout, error between connect and
full response, proxy failure. Plus an errorstring good enough for logging.

'''

import time
import traceback
from collections import namedtuple
import ssl
import urllib
import json
from pprint import pformat

import asyncio
import logging
import aiohttp


from . import stats
from . import config
from . import content

LOGGER = logging.getLogger(__name__)

def _generate_form_data(**kwargs):

    multipart = kwargs.pop('multipart_post')

    form_data = aiohttp.FormData()

    for name, value in kwargs.items():
        data = []
        if isinstance(value, (list, dict)):
            data = [name, json.dumps(value)]

        elif isinstance(value, (int, float, str, bool)):
            data = [name, str(value)]

        elif value is None:
            data = [name, '']
        else:
            raise TypeError("Unknown Type: {}".format(type(value)))

        if multipart:
            form_data.add_field(*data,content_type="multipart/form-data")
        else:
            form_data.add_field(*data)


    return form_data
# these errors get printed deep in aiohttp but they also bubble up
aiohttp_errors = {
    'SSL handshake failed',
    'SSL error errno:1 reason: CERTIFICATE_VERIFY_FAILED',
    'SSL handshake failed on verifying the certificate',
    'Fatal error on transport TCPTransport',
    'Fatal error on SSL transport',
    'SSL error errno:1 reason: UNKNOWN_PROTOCOL',
    'Future exception was never retrieved',
    'Unclosed connection',
    'SSL error errno:1 reason: TLSV1_UNRECOGNIZED_NAME',
    'SSL error errno:1 reason: SSLV3_ALERT_HANDSHAKE_FAILURE',
    'SSL error errno:1 reason: TLSV1_ALERT_INTERNAL_ERROR',
    'SSL error errno:1 reason: WRONG_VERSION_NUMBER',
    'SSL error errno:1 reason: TLSV1_ALERT_PROTOCOL_VERSION'
}


class AsyncioSSLFilter(logging.Filter):
    def filter(self, record):
        if record.name == 'asyncio' and record.levelname == 'ERROR':
            msg = record.getMessage()
            for ae in aiohttp_errors:
                if msg.startswith(ae):
                    return False
        return True


def establish_filters():
    f = AsyncioSSLFilter()
    logging.getLogger('asyncio').addFilter(f)


# XXX should be a policy plugin
# XXX cookie handling -- no way to have a cookie jar other than at session level
#    need to directly manipulate domain-level cookie jars to get cookies
def apply_url_policies(url, crawler):
    headers = {}
    proxy = None
    mock_url = None
    mock_robots = None

    headers['User-Agent'] = crawler.ua

    test_host = config.read('Testing', 'TestHostmapAll')
    if test_host:
        headers['Host'] = url.urlsplit.netloc
        (scheme, netloc, path, query, fragment) = url.urlsplit
        netloc = test_host
        mock_url = urllib.parse.urlunsplit((scheme, netloc, path, query, fragment))
        mock_robots = url.urlsplit.scheme + '://' + test_host + '/robots.txt'

    if crawler.prevent_compression:
        headers['Accept-Encoding'] = 'identity'
    else:
        headers['Accept-Encoding'] = content.get_accept_encoding()

    if crawler.upgrade_insecure_requests:
        headers['Upgrade-Insecure-Requests'] = '1'

    return headers, proxy, mock_url, mock_robots


FetcherResponse = namedtuple('FetcherResponse', ['response', 'body_bytes', 'req_headers',
                                                 't_first_byte', 't_last_byte', 'is_truncated',
                                                 'last_exception'])


async def fetch(url, session,req=None, headers=None, proxy=None, mock_url=None,dns_entry=None,
                allow_redirects=None, max_redirects=None,
                stats_prefix='', max_page_size=-1):

    dns_log = (0 , 'no_dns')
    if dns_entry:
        addrs, expires, _, host_geoip = dns_entry
        if isinstance(addrs,list):
            records_num = len(addrs)
            if records_num > 0:
                dns_log = (records_num, str(addrs[0]))
            else:
                dns_log = (0,str(addrs))
        else:
            dns_log = (0,str(addrs))

    # fr = FetcherResponse('response', 'body_bytes', 'response.request_info.headers',
    #                      't_first_byte', 't_last_byte', 'is_truncated', None)
    #
    if proxy:  # pragma: no cover
        proxy = aiohttp.ProxyConnector(proxy=proxy)
        # XXX we need to preserve the existing connector config (see cocrawler.__init__ for conn_kwargs)
        # XXX we should rotate proxies every fetch in case some are borked
        # XXX use proxy history to decide not to use some
        raise ValueError('not yet implemented')

    last_exception = None
    is_truncated = False

    try:
        t0 = time.time()
        last_exception = None
        body_bytes = b''
        blocks = []
        left = max_page_size

        with stats.coroutine_state(stats_prefix+'fetcher fetching'):
            with stats.record_latency(stats_prefix+'fetcher fetching', url=url.url):

                if req.headers and len(req.headers):
                    if headers is None:
                        headers = req.headers
                    else:
                        headers.update(req.headers)

                if req.cookies is not None:

                    if isinstance(session.cookie_jar,aiohttp.DummyCookieJar):
                        raise ValueError('--> Trying to set cookie but cookiejar is dummy, enable --reuse_session for cruzer!')

                    session.cookie_jar.update_cookies(req.cookies)

                if req.post is not None:
                    if req.multipart_post:
                        post_data = _generate_form_data(multipart_post=req.multipart_post,**req.post)
                    else:
                        post_data = req.post
                else:
                    post_data = None


                response = await session.request(req.method,mock_url or url.url,
                                         allow_redirects=allow_redirects,
                                         max_redirects=max_redirects,
                                         headers=headers,data=post_data)


                # https://aiohttp.readthedocs.io/en/stable/tracing_reference.html
                # XXX should use tracing events to get t_first_byte
                t_first_byte = '{:.3f}'.format(time.time() - t0)

                while left > 0:
                    block = await response.content.read(left)
                    if not block:
                        body_bytes = b''.join(blocks)
                        break
                    blocks.append(block)
                    left -= len(block)
                else:
                    body_bytes = b''.join(blocks)

                if not response.content.at_eof():
                    stats.stats_sum('fetch truncated length', 1)
                    response.close()  # this does interrupt the network transfer
                    is_truncated = 'length'  # testme WARC

                t_last_byte = '{:.3f}'.format(time.time() - t0)
    except asyncio.TimeoutError as e:
        stats.stats_sum('fetch timeout', 1)
        last_exception = 'TimeoutError'
        body_bytes = b''.join(blocks)
        if len(body_bytes):
            is_truncated = 'time'  # testme WARC
            stats.stats_sum('fetch timeout body bytes found', 1)
            stats.stats_sum('fetch timeout body bytes found bytes', len(body_bytes))
#    except (aiohttp.ClientError.ClientResponseError.TooManyRedirects) as e:
#        # XXX remove me when I stop using redirects for robots.txt fetching
#        raise
    except (aiohttp.ClientError) as e:
        # ClientError is a catchall for a bunch of things
        # e.g. DNS errors, '400' errors for http parser errors
        # ClientConnectorCertificateError for an SSL cert that doesn't match hostname
        # ClientConnectorError(None, None) caused by robots redir to DNS fail
        # ServerDisconnectedError(None,) caused by servers that return 0 bytes for robots.txt fetches
        # TooManyRedirects("0, message=''",) caused by too many robots.txt redirs

        stats.stats_sum('fetch ClientError', 1)
        detailed_name = str(type(e).__name__)
        last_exception = 'ClientError: ' + detailed_name + ': ' + str(e) + ': dns [0]: {1}'.format(dns_log[0],dns_log[1])
        body_bytes = b''.join(blocks)
        if len(body_bytes):
            is_truncated = 'disconnect'  # testme WARC
            stats.stats_sum('fetch ClientError body bytes found', 1)
            stats.stats_sum('fetch ClientError body bytes found bytes', len(body_bytes))
    except ssl.CertificateError as e:
        # unfortunately many ssl errors raise and have tracebacks printed deep in aiohttp
        # so this doesn't go off much
        stats.stats_sum('fetch SSL error', 1)
        last_exception = 'CertificateError: ' + str(e)
    #except (ValueError, AttributeError, RuntimeError) as e:
        # supposedly aiohttp 2.1 only fires these on programmer error, but here's what I've seen in the past:
        # ValueError Location: https:/// 'Host could not be detected' -- robots fetch
        # ValueError Location: http:// /URL should be absolute/ -- robots fetch
        # ValueError 'Can redirect only to http or https' -- robots fetch -- looked OK to curl!
        # AttributeError: ?
        # RuntimeError: ?
    except ValueError as e:
        # no A records found -- raised by my dns code
        stats.stats_sum('fetch other error - ValueError', 1)
        last_exception = 'ValueErorr: ' + str(e)
    except AttributeError as e:
        stats.stats_sum('fetch other error - AttributeError', 1)
        last_exception = 'AttributeError: ' + str(e)
    except RuntimeError as e:
        stats.stats_sum('fetch other error - RuntimeError', 1)
        last_exception = 'RuntimeError: ' + str(e)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        last_exception = 'Exception: ' + str(e)
        stats.stats_sum('fetch surprising error', 1)
        LOGGER.info('Saw surprising exception in fetcher working on %s:\n%s', mock_url or url.url, last_exception)
        traceback.print_exc()



    if last_exception is not None:
        LOGGER.debug('we failed working on %s, the last exception is %s', mock_url or url.url, last_exception)
        return FetcherResponse(None, None, None, None, None, False, last_exception)

    fr = FetcherResponse(response, body_bytes, response.request_info.headers,
                         t_first_byte, t_last_byte, is_truncated, None)

    if response.status >= 500:
        LOGGER.debug('server returned http status %d', response.status)

    stats.stats_sum('fetch bytes', len(body_bytes) + len(response.raw_headers))

    stats.stats_sum(stats_prefix+'fetch URLs', 1)
    stats.stats_sum(stats_prefix+'fetch http code=' + str(response.status), 1)

    # checks after fetch:
    # hsts header?
    # if ssl, check strict-transport-security header, remember max-age=foo part., other stuff like includeSubDomains
    # did we receive cookies? was the security bit set?

    log_headers = None #pformat(dict(response.raw_headers),indent=10)
    LOGGER.debug('<{0} [{1}] {2}   dns [3]: {4}> \n {5}'.format(req.method,
                                                            response.status,
                                                            url.url,
                                                            dns_log[0],
                                                            dns_log[1],
                                                            log_headers or ''
                                                            ))

    return fr


def upgrade_scheme(url):
    '''
    Upgrade crawled scheme to https, if reasonable. This helps to reduce MITM attacks against the crawler.

    https://chromium.googlesource.com/chromium/src/net/+/master/http/transport_security_state_static.json

    Alternately, the return headers from a site might have strict-transport-security set ... a bit more
    dangerous as we'd have to respect the timeout to avoid permanently learning something that's broken

    TODO: use HTTPSEverwhere? would have to have a fallback if https failed, which it occasionally will
    '''
    return url
