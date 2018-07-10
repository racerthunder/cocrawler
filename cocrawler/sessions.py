import time
import logging
from collections import defaultdict
import asyncio

LOGGER = logging.getLogger(__name__)

class SessionPool():
    def __init__(self):
        self._pool = {} # {id:{data_dict(session:session_object,**kwargs)}}
        self._tasks_submited = defaultdict(list) # session_id = [tasks names]
        self._tasks_finished = defaultdict(list) # session_id = [tasks names]
        self._global_session = None # this session is used when reuse_session is Fals


    def size(self):
        return len(self._pool)

    def _glob_getter(self):
        if self._global_session is None:
            raise ValueError('--> Global session is not set')
        return self._global_session

    def _glob_setter(self,val):
        if val is None:
            raise ValueError('--> Bad value for global session: {0}'.format(val))

        self._global_session = val


    global_session = property(_glob_getter,_glob_setter)


    def add_session(self,id,session,**kwargs):
        '''
        adds configured client session to the pool with given id which is linked in Task
        :param id:
        :param session:
        :return:
        '''
        if self._pool.get(id,None) is not None:
            raise ValueError('--> Session with id: {0} already exists!'.format(id))

        data = {'session':session}
        data.update(**kwargs)

        self._pool[id]=data


    def get_session(self,id):
        if id is None:
            # we need global session here
            return self.global_session

        _s = self._pool.get(id,None)

        if _s is None:
            raise ValueError('--> Session with id: {0} not found'.format(id))

        return _s['session']


    def add_submited_task(self,session_id,name):
        self._tasks_submited[session_id].append(name)


    def add_finished_task(self,session_id,name):
        self._tasks_finished[session_id].append(name)

    async def close_and_delete(self,session_id):
        session_data = self._pool.get(session_id)
        await session_data['session'].close()
        del self._pool[session_id]
        LOGGER.debug('--> Session: {0} has been deleted for url: {1}'.format(session_id,session_data['url']))

    async def close_or_wait(self):
        '''
        run thru all open session and check difference, if list are identical close the session
        :param data:
        :return:
        '''
        data = self.sessions_checker()

        for session_id,diff in data.items():

            if len(diff)==0:
                LOGGER.debug('--> Session: {0} finished all task'.format(session_id))
                await self.close_and_delete(session_id)

            else:
                #give back control if all sessions are busy
                LOGGER.debug('--> Session: {0} has running tasks: {1}'.format(session_id,str(diff)))
                await asyncio.sleep(0.1)


    def sessions_checker(self):
        '''
        run thru all open session and compare open and closed tasks
        :return dict(session_id : diff_list)

        '''
        data = {}
        for session_id,session_data in self._pool.items():
            submited = set(self._tasks_submited[session_id])
            finished = set(self._tasks_finished[session_id])
            diff = submited.difference(finished)
            # print(f'{session_id} ({session_data["url"]}), submited:{submited} , finished:{finished}, diff: {diff}')
            data[session_id]=diff


        return data
