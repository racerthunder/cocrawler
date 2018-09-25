from cocrawler.dns_warmup import Warmupper
import cocrawler.config as config
import argparse
import logging

ARGS = argparse.ArgumentParser(description='Cruzer web crawler')

ARGS.add_argument('--loglevel', action='store', default='DEBUG')
ARGS.add_argument('--reuse_session',action='store_true')
ARGS.add_argument('--config', action='append')
ARGS.add_argument('--configfile', action='store')
ARGS.add_argument('--no-confighome', action='store_true')
ARGS.add_argument('--no-test', action='store_true')
ARGS.add_argument('--printdefault', action='store_true')
ARGS.add_argument('--load', action='store',help='load previous state of the parser')

args = ARGS.parse_args()

config.config(args.configfile, args.config)

logging.basicConfig(level=logging.NOTSET)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(level=logging.INFO)

def main():
    ns_alive = Warmupper().looper()
    print(ns_alive)

if __name__ == '__main__':
    main()
