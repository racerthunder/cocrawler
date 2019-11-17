import pickle
import logging
import cachetools.ttl
import peewee_async
from copy import deepcopy
import peewee
import async_timeout

from . import config
from . import memory

LOGGER = logging.getLogger(__name__)
__NAME__ = 'datalayer seen memory'


class SqlHelper():
    def __init__(self, manager):
        self.manager = manager


    async def exists(self, query):
        try:
            res = await self.manager.get(query)
        except peewee.DoesNotExist:
            res = None

        return False if res is None else True


class Datalayer:
    def __init__(self,cocrawler):
        self.cocrawler = cocrawler

        self.seen_set = set()

        robots_size = config.read('Robots', 'RobotsCacheSize')
        robots_ttl = config.read('Robots', 'RobotsCacheTimeout')
        self.robots = cachetools.ttl.TTLCache(robots_size, robots_ttl)

        memory.register_debug(self.memory)

        self._a_manager = None

    def peewee_setter(self,value):
        '''

        :param value: <str> or <tuple: database, logging level(default:logging.DEBUG)>
        :return:
        '''

        def get_async_params(database):
            __params = deepcopy(database.__dict__.get('connect_params') or database.__dict__.get('connect_kwargs'))
            if __params is None:
                raise KeyError('--> db params cant be None ')
            if 'password' not in __params and 'passwd' not in __params:
                raise ValueError('--> No password found in connection string')

            if 'password' not in __params:
                __params['password'] = __params['passwd']
            __params['db_name']=database.database

            return __params

        if isinstance(value,tuple):
            value, logging_level = value
            logger_async = logging.getLogger('peewee.async')
            logger_async.setLevel(logging_level)

            logger_peewee = logging.getLogger('peewee')
            logger_peewee.setLevel(logging_level)

        if self._a_manager is None:
            async_connect_params = get_async_params(value)

            a_database = peewee_async.PooledMySQLDatabase(async_connect_params['db_name'],
                                                    host=async_connect_params['host'],
                                                    port=async_connect_params['port'],
                                                    user=async_connect_params['user'],
                                                    password=async_connect_params['password'],
                                                    max_connections=100,
                                                    pool_recycle=30)


            a_manager = peewee_async.Manager(a_database, loop=self.cocrawler.loop)

            a_manager.database.allow_sync = False

            self._a_manager = a_manager
            setattr(self._a_manager, 'sql', SqlHelper(self._a_manager)) # helper shortcut
            setattr(self.cocrawler, 'peewee', self._a_manager) # shortcut for quick access


    def peewee_getter(self):
        if self._a_manager is None:
            raise ValueError('--> First assign peewee database via self.datalayer.peewee=DATABASE')
        return self._a_manager

    peewee = property(peewee_getter,peewee_setter)

    def add_seen(self, url):
        '''A "seen" url is one that we've done something with, such as having
        queued it or already crawled it.'''
        self.seen_set.add(url.surt)

        if config.read('Fetcher', 'CleanClosedSSL'):
            if not self.cocrawler.conn_kwargs['enable_cleanup_closed']:
                raise ValueError('--> CleanClosedSSL requires [enable_cleanup_closed] to be True for Connector')

            if len(self.seen_set) % self.cocrawler.cleanup_ssl_every == 0:
                self.cocrawler.connector._cleanup()
                transports = len(self.cocrawler.connector._cleanup_closed_transports)
                self.cocrawler.connector._cleanup_closed()
                LOGGER.info('--> SSL Cleaned up {0} ssl connections'.format(transports))

    def seen(self, url):
        return url.surt in self.seen_set

    def cache_robots(self, schemenetloc, parsed):
        self.robots[schemenetloc] = parsed

    def read_robots_cache(self, schemenetloc):
        return self.robots[schemenetloc]

    def save(self, f):
        pickle.dump(__NAME__, f)
        pickle.dump(self.seen_set, f)
        # don't save robots cache

    def load(self, f):
        name = pickle.load(f)
        if name != __NAME__:
            LOGGER.error('save file name does not match datalayer name: %s != %s', name, __NAME__)
            raise ValueError
        self.seen_set = pickle.load(f)

    def summarize(self):
        '''Print a human-readable sumary of what's in the datalayer'''
        print('{} seen'.format(len(self.seen_set)))

    def memory(self):
        '''Return a dict summarizing the datalayer's memory usage'''
        seen_set = {}
        seen_set['bytes'] = memory.total_size(self.seen_set)
        seen_set['len'] = len(self.seen_set)
        robots = {}
        robots['bytes'] = memory.total_size(self.robots)
        robots['len'] = len(self.robots)
        return {'seen_set': seen_set, 'robots': robots}
