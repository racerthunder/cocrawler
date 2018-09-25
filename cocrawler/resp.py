import attr
from .urls import URL

@attr.s(frozen=True)
class Resp():
    url = attr.ib()
    status = attr.ib()
    headers = attr.ib()
    raw_headers = attr.ib()

    @status.validator
    def status_validator(self,attrib,value):
        if not isinstance(value,(str,int,None.__class__)):
            raise ValueError('--> status must be (str,int,None)')


