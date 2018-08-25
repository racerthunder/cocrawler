import asyncio
import logging
import cocrawler.dns as dns
import cocrawler.config as config
from pathlib import Path
import datetime
import pickle

logging.basicConfig(level=logging.DEBUG)

config.config('/Volumes/crypt/_Coding/PYTHON/cocrawler/configs/main.yml', None)

files = config.read('Fetcher', 'Nameservers').get('File')

ns_list = []

config_dir =  Path(__file__).parent.parent / 'data'


for file_name in files:

    full_file_path = config_dir / file_name
    ls = [line.strip() for line in full_file_path.open(encoding='utf-8') if len(line)>1 and '#' not in line]
    ns_list.extend(ls)


async def resolve(ns):
    host = 'mail.ru'
    dns.setup_resolver([ns])
    try:
        result = await dns.query(host, 'A')
        print(host, result)
    except Exception as e:
        result = None
        print('saw exception: {0}, ns = {1}'.format(e,ns))
    return ns,result

async def runner():
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


def main():

    loop = asyncio.get_event_loop()

    loop.run_until_complete(runner())


def misc():
    config_dir =  Path(__file__).parent.parent / 'data'
    print(config)
    return 
    dns_warmup_path = config_dir / 'dns_warmup.pickle'
    #dns_file = open(str(dns_warmup_path),'wb')
    # date = datetime.datetime.today()
    #
    # pickle.dump(date,dns_file)
    # dns_file.close()



    fileObject = open(str(dns_warmup_path),'rb')
    date_saved = pickle.load(fileObject)
    print(date_saved)

    back_date = datetime.datetime.strptime('Aug 20 2018  1:33PM', '%b %d %Y %I:%M%p')

    delta = date_saved - back_date

    print(delta.days)

if __name__ == '__main__':
    #main()
    misc()

