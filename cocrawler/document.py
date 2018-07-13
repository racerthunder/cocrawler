import defusedxml.lxml
from lxml.html import HTMLParser,HtmlElement
from selection import XpathSelector
from six import BytesIO, StringIO

'''
https://github.com/rushter/selectolax - faster 20 times then lxml
'''

class Document():
    def __init__(self,html=None):
        self._html = html
        self._etree = None
        self._fetcher = None # fetcher complete response object
        self.status = None # (int or last_exception) status taken from fetcher response object for quick access

    def parse(self,html=None):
        """
        optionaly parse html to etree on demand, to delegate work to burner
        :param html:
        :return:
        """
        _html = self._html if not html else html
        if _html is None:
            raise ValueError('--> No html, pass it to the init or parse function')

        dom = defusedxml.lxml.parse(StringIO(_html),
                                    parser=HTMLParser())
        etree = dom.getroot()

        self._etree = etree
        return etree

    def select(self, *args, **kwargs):
        if self.etree is None:
            self.parse()
        return XpathSelector(self._etree).select(*args, **kwargs)


    def _get_etree(self):
        return self._etree

    def _set_etree(self, obj):
        if isinstance(obj,HtmlElement):
            self._etree = obj
        else:
            raise ValueError('--> object is not HtmlElement')


    etree = property(_get_etree, _set_etree)


    def _get_html(self):
        return self._html

    def _set_html(self, html):
        self._html = html

    html = property(_get_html, _set_html)


    def _get_fetcher(self):
        return self._fetcher

    def _set_fetcher(self, fetcher):
        self._fetcher = fetcher

    #fetcher complete response object
    fetcher = property(_get_fetcher, _set_fetcher)

def main():
    from pathlib import Path
    path = Path('/Volumes/crypt/_Coding/PYTHON/TEMP/buffer.html')
    with path.open(encoding='utf-8') as f:
        html = f.read()

    doca = Document()
    doca.parse(html)

    # for item in doca.select('//a'):
    #     print(item.html())



if __name__ == '__main__':
    main()

