from cocrawler.req import Req




def misc():

    req = Req('http://tut.by')
    req.url = 'http://google.com'
    print(req.url)
if __name__ == '__main__':
    #main()
    misc()
