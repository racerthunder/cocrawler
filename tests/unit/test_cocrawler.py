'''
Most testing of cocrawler is done by fake crawling, but there are a few things...
'''

import asyncio
import tempfile
import os

import cocrawler
import cocrawler.config as config
from cocrawler.urls import URL


def test_cocrawler(capsys):
    config.config(None, None, confighome=False)

    # we have to get around the useragent checks
    config.write('pytest', 'UserAgent', 'MyPrefix')
    config.write('http://example.com/pytest-test-cocrawler.py', 'UserAgent', 'URL')
    # and configure url_allowed
    config.write('AllDomains', 'Plugins', 'url_allowed')

    crawler = cocrawler.Crawler()

    crawler.add_url(0, {'url': URL('http://tut.by/')})
    crawler.add_url(0, {'url': URL('http://habr.com/')})

    assert crawler.qsize == 3

    f = tempfile.NamedTemporaryFile(delete=False)
    name = f.name

    with open(name, 'wb') as f:
        crawler.save(f)
    assert crawler.qsize == 0

    crawler.add_url(0, {'url': URL('http://example4.com/')})
    assert crawler.qsize == 1

    with open(name, 'rb') as f:
        crawler.load(f)

    assert crawler.qsize == 3

    os.unlink(name)
    assert not os.path.exists(name)

    # clear out the existing capture
    out, err = capsys.readouterr()

    crawler.summarize()

    out, err = capsys.readouterr()

    assert err == ''
    assert len(out) >= 200  # not a very good test, but at least it is something


def misc():
    config.config(None, None, confighome=False)

        # we have to get around the useragent checks
    config.write('pytest', 'UserAgent', 'MyPrefix')
    config.write('http://example.com/pytest-test-cocrawler.py', 'UserAgent', 'URL')
    # and configure url_allowed
    config.write('AllDomains', 'Plugins', 'url_allowed')

    crawler = cocrawler.Crawler()

    crawler.add_url(0, {'url': URL('http://tut.by/')})
    crawler.add_url(0, {'url': URL('http://habr.com/')})

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.crawl())

    crawler.summarize()


def misc2():
    import psutil
    p = psutil.Process()
    from multiprocessing import cpu_count

    p = psutil.Process(os.getpid())
    try:
        p.set_cpu_affinity(list(range(cpu_count())))
    except:
        pass

if __name__ == '__main__':
    misc()
    #misc2()
    pass
