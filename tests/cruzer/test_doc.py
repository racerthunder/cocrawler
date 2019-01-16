from cocrawler.document import Document
from cocrawler.req import Req
from pathlib import Path

def main():

    path = Path('./doc_buffer.html')
    with path.open(encoding='utf-8') as f:
        html = f.read()


    doca = Document(html)


    # for item in doca.select('//a'):
    #     print(item.html())
    doca.set_input('txt','lemoncayennepepperdiet.com')
    req = doca.get_req()
    print('post: ',req.post)
    print('url: ',req.url.url)
    #print(render_html(doca.form))

def test_task():
    from cocrawler.task import Task
    req = Req('http://google.com')
    t = Task(name='name', req=req)
    print(t.doc.status)
    if t.doc.status == 200:
        print('ok')

if __name__ == '__main__':
    #main()
    test_task()
