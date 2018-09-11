from functools import wraps
import logging
from .urls import URL



_logger = logging.getLogger(__name__)

def proxy_checker(proxy,proxy_token,logger=None):
    def proxy_inner(method):
        @wraps(method)
        def _impl(self,task):
            LOGGER = logger or _logger
            if task.doc.html is None or (proxy_token not in task.doc.html):
                LOGGER.debug('--> Bad proxy for task: {0}'.format(task.name))
                new_url = proxy.get_next_proxy_cycle(task.req.source_url)
                task_clone = task.clone_task()
                task_clone.req.url = URL(new_url)
                yield task_clone
                yield StopIteration()

            else:
                LOGGER.debug('--> proxy is alive task: {0}'.format(task.name))

                result = method(self, task)

                try:
                    _it = iter(result)

                except TypeError:
                    yield StopIteration()

                else:
                    for _t in _it:
                        yield _t
                    else:
                        # make run function of Crawler think we are done with the job
                        yield StopIteration()


        return _impl

    return proxy_inner
