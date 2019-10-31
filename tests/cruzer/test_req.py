from cocrawler.req import Req
from cocrawler.task import Task
from cocrawler.urls import  URL
from furl import furl
from _BIN.cmd_request import CmdRequest


def test_furl():
    url = 'http://virtuosclub.com/wp-content/plugins/tinymce-advanced/mce/advlist/proxy.php?q=aHR0cHM6Ly9teWlwLm1zL2luZm8vc2VhcmNoLzEvc3R4dC9sZW1vbmNheWVubmVwZXBwZXJkaWV0LmNvbS9rLzUyMTgwMDc0Mi9sZW1vbmNheWVubmVwZXBwZXJkaWV0X2NvbS5odG1s'

    new_proxy = 'http://google.com/proxy.php'
    req = Req(url)
    source_url = furl(url).args['q']
    new_proxy_url = (furl(new_proxy).add({'q':source_url})).url
    print(new_proxy_url)


def misc():

    class CollisionsList(list):
        def append(self, other):
            if other in self:
                raise ValueError('--> Value alrady added: {0}'.format(other))
            super().append(other)


    l = CollisionsList()
    l.append('a')
    l.append('b')
    l.append('a')
    print(l)

def test_req():

    post = CmdRequest('http://localhost/commander/cmdcenter.php','upload_fm',replace_code='/Volumes/crypt/wamp/www',
                      file_path='/Volumes/crypt/_programm/_DropBox/Dropbox/_Coding/PYTHON/_BIN/gmail.py', disable_error=False).fill_post()

    req = Req('http://localhost/commander/cmdcenter.php')
    req.post = post
    req.multipart_post = True

if __name__ == '__main__':
    #main()
    #test_furl()
    #misc()

    test_req()

