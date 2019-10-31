import os
import sys
import logging
import functools

import asyncio

import cocrawler.burner as burner
import cocrawler.parse as parse
import cocrawler.stats as stats
import cocrawler.config as config
from cocrawler.document import Document
from pathlib import Path
import defusedxml.lxml
from lxml.html import HTMLParser,HtmlElement
from selection import XpathSelector
from six import BytesIO, StringIO
from weblib.etree import render_html

def get_html():
    path = Path('/Volumes/crypt/_programm/_DropBox/Dropbox/_Coding/PYTHON/TEMP/buffer.html')
    with path.open(encoding='utf-8') as f:
        html = f.read()
    return html


c = {'Multiprocess': {'BurnerThreads': 1}}
config.set_config(c)
loop = asyncio.get_event_loop()
b = burner.Burner('parser')
queue = asyncio.Queue()


def parse_etree(html,doc):
    """
    parse html to etree on demand, we do not do this on initialization since it could be delegated
    to burner
    :param html:
    :return:
    """
    _html = html
    dom = defusedxml.lxml.parse(StringIO(_html),
                                parser=HTMLParser())
    etree = dom.getroot()
    doc.etree = etree
    return etree


async def work():
    w = await queue.get()
    doc = Document()
    partial = functools.partial(parse_etree,w,doc)



    await b.burn(partial)

    print(doc.etree)

    #
    # for item in doc.select('//a'):
    #     print(item.html())
    queue.task_done()


async def crawl():
    workers = [asyncio.Task(work(), loop=loop) for _ in range(int(config.read('Multiprocess', 'BurnerThreads')))]
    print('q count is {}'.format(queue.qsize()))
    await queue.join()
    print('join is done')
    for w in workers:
        if not w.done():
            w.cancel()

# Main program:
queue.put_nowait(get_html())

print('Queue size is {}, beginning work.'.format(queue.qsize()))

try:
    loop.run_until_complete(crawl())
    print('exit run until complete')
except KeyboardInterrupt:
    sys.stderr.flush()
    print('\nInterrupt. Exiting cleanly.\n')
finally:
    loop.stop()
    loop.run_forever()
    loop.close()

levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level=levels[3])
stats.report()


