import time
import logging
from collections import defaultdict
import asyncio
from statistics import mean

LOGGER = logging.getLogger(__name__)

class SessionPool():

    def __init__(self,max_work_time=10):
        '''

        :param max_work_time: int, multiplier for avg_work_time
        '''
        self._pool = {} # {id:{data_dict(session:session_object,**kwargs)}}
        self._tasks_submited = defaultdict(list) # session_id = [(task_names,start_time)]
        self._tasks_finished = defaultdict(list) # session_id = [(task_names,finish_time)]
        self._global_session = None # this session is used when reuse_session is False
        self._work_times = [] # record time completion in seconds for each task
        self.max_work_time = max_work_time # in seconds (avg_work_time * max_work_time)
        self._last_avg_work_time = None

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

        data = {'session':session,'time_start':time.time()}
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
        self._tasks_submited[session_id].append((name,time.time()))


    def add_finished_task(self,session_id,name):
        self._tasks_finished[session_id].append((name,time.time()))

    async def close_and_delete(self,session_id):
        session_data = self._pool.get(session_id)

        # add total time to completion to the list
        work_time = time.time() - session_data['time_start']
        self._work_times.append(work_time)
        #

        await session_data['session'].close()

        del self._pool[session_id]
        del self._tasks_submited[session_id]
        del self._tasks_finished[session_id]

        LOGGER.debug('--> Session: {0} has been deleted for url: {1}'.format(session_id,session_data['url']))

    def _get_avg_work_time(self):
        '''
        calculate avarage working time for each session in seconds, use only last 100 measures
        :return:
        '''
        if len(self._work_times) > 100:
            # get last 100 items to and truncate the list
            self._work_times = self._work_times[-100:]
            avg = mean(self._work_times)

        elif len(self._work_times)==0:
            avg = None
        else:
            avg = mean(self._work_times)

        return avg



    async def close_or_wait(self):
        '''
        run thru all open session and check difference, if list are identical close the session
        :param data:
        :return:
        '''

        self._last_avg_work_time = self._get_avg_work_time()

        data = self.sessions_checker()

        curr_time = time.time()

        for session_id,diff in data.items():
            session_data = self._pool.get(session_id)
            # check if session has stalled by checking last completed task time
            if self._last_avg_work_time is not None:
                if len(self._tasks_finished[session_id]) > 0:

                    last_task_time = self._tasks_submited[session_id][-1][1] # get time for the last submited task
                    running_time = curr_time - last_task_time
                    if running_time > (self._last_avg_work_time * self.max_work_time):
                        LOGGER.warning('--> Session: {0}, for url {1}, has stalled, running time: {2}'.format(session_id,session_data['url'],str(running_time)))
                        await self.close_and_delete(session_id)
                        continue
            # end

            if len(diff)==0:
                LOGGER.debug('--> Session: {0} finished all task'.format(session_id))
                await self.close_and_delete(session_id)

            else:
                #give back control if all sessions are busy
                LOGGER.debug('--> Session: {0} has running tasks: {1} , url: {2}'.format(session_id,str(diff),session_data['url']))
                await asyncio.sleep(0.1)


    def sessions_checker(self):
        '''
        run thru all open session and compare open and closed tasks
        :return dict(session_id : diff_list)

        '''
        data = {}
        for session_id,session_data in self._pool.items():
            submited = set([name for name,time in self._tasks_submited[session_id]])
            finished = set([name for name,time in self._tasks_finished[session_id]])
            diff = submited.difference(finished)
            # print(f'{session_id} ({session_data["url"]}), submited:{submited} , finished:{finished}, diff: {diff}')
            data[session_id]=diff


        return data

    @property
    def busy(self):
        if len(self._tasks_submited) > 0:
            return True
        else:
            return False
