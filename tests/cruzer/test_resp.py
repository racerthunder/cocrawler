from cocrawler.resp import Resp
from cocrawler.urls import URL

def main():
    url = URL('http://google.com')
    response = Resp(url,300,'dd','dd')

    print(response)

if __name__ == '__main__':
    main()
