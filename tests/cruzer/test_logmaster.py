from cocrawler.log_master import LogMaster
import asyncio

async def main():
    master = LogMaster('cruz')
    await master.write('/Volumes/crypt/_programm/_DropBox/Dropbox/_Coding/PYTHON/cocrawler/log/test.log.txt','linetowrite')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

