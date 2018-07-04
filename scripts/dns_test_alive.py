import asyncio
import logging
import cocrawler.dns as dns
import cocrawler.config as config
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

config.config('/Volumes/crypt/_Coding/PYTHON/cocrawler/configs/main.yml', None)

files = config.read('Fetcher', 'Nameservers').get('File')

ns_list = []


for file_name in files:
    config_dir =  Path(__file__).parent.parent / 'data'
    full_file_path = config_dir / file_name
    ls = [line.strip() for line in full_file_path.open(encoding='utf-8') if len(line)>1 and '#' not in line]
    ns_list.extend(ls)


async def resolve(ns):
    host = 'tradingview.com'
    dns.setup_resolver([ns])
    try:
        result = await dns.query(host, 'A')
        print(host, result)
    except Exception as e:
        result = None
        print('saw exception: {0}, ns = {1}'.format(e,ns))
    return ns,result

async def main():
    tasks = []
    good = []
    bad = []
    for ns in ns_list:
        tasks.append(asyncio.Task(resolve(ns)))

    for t in asyncio.as_completed(tasks):
        ns,res = await t
        if res:
            print((res[0].host))
            good.append(ns)
        else:
            bad.append(ns)

    print('--> bad: {0}'.format('~'.join(bad)))
    print('--> good: {0}'.format('~'.join(good)))



loop = asyncio.get_event_loop()

loop.run_until_complete(main())
