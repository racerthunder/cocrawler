from bs4 import BeautifulSoup

import cocrawler.parse as parse
from cocrawler.urls import URL

test_html = '''
<html>
<head><title>Foo</title><link href='link.html'></link></head>
<body>
<a href="foo1.html">Anchor 1</a>
<a
href=foo2.htm>Anchor 2</a>
<a
 href="foo3.html ">Anchor 3</a>
<img src=foo.gif />
</body>
'''

test_html_no_body = '''
<html>
<head><title>Foo</title><link href='link.html'></link></head>
<a href="foo1.html">Anchor 4</a>
<a
href=foo2.htm>Anchor 5</a>
<a
 href="foo3.html ">Anchor 6</a>
<img src=foo.gif />
'''

test_html_no_head = '''
<html>
<body>
<a href="foo1.html">Anchor 7</a>
<a
href=foo2.htm>Anchor 8</a>
<a
 href="foo3.html ">Anchor 9</a>
<img src=foo.gif />
</body>
'''

test_html_no_nothing = '''
<a href="foo1.html">Anchor 10</a>
<a
href=foo2.htm>Anchor 11</a>
<a
 href="foo3.html ">Anchor 12</a>
<img src=foo.gif />
'''


def test_do_burner_work_html():
    urlj = URL('http://example.com')
    test_html_bytes = test_html.encode(encoding='utf-8', errors='replace')
    headers = {}
    links, embeds, sha1, facets = parse.do_burner_work_html(test_html, test_html_bytes, headers, url=urlj)
    assert len(links) == 3
    assert len(embeds) == 2
    linkset = set(u.url for u in links)
    embedset = set(u.url for u in embeds)
    assert 'http://example.com/foo3.html' in linkset
    assert 'http://example.com/foo.gif' in embedset
    assert sha1 == 'sha1:3458e0857ec379ec56d4c7fb39d33c90c8b5ae93'

    # as a handwave, let's expect these defective pages to also work.

    test_html_bytes = test_html_no_body.encode(encoding='utf-8', errors='replace')
    links, embeds, sha1, facets = parse.do_burner_work_html(test_html_no_body, test_html_bytes, headers, url=urlj)
    assert len(links) == 3
    assert len(embeds) == 2

    test_html_bytes = test_html_no_head.encode(encoding='utf-8', errors='replace')
    links, embeds, sha1, facets = parse.do_burner_work_html(test_html_no_head, test_html_bytes, headers, url=urlj)
    assert len(links) == 3
    assert len(embeds) == 1

    test_html_bytes = test_html_no_nothing.encode(encoding='utf-8', errors='replace')
    links, embeds, sha1, facets = parse.do_burner_work_html(test_html_no_nothing, test_html_bytes, headers, url=urlj)
    assert len(links) == 3
    assert len(embeds) == 1


def test_individual_parsers():
    links, embeds = parse.find_html_links_re(test_html)
    assert len(links) == 5
    assert len(embeds) == 0
    assert 'foo3.html ' in links
    assert 'foo.gif' in links

    head, body = parse.split_head_body_re(test_html)
    links, embeds = parse.find_body_links_re(body)
    assert len(links) == 3
    assert len(embeds) == 1
    assert 'foo3.html ' in links
    assert 'foo.gif' in embeds

    head_soup = BeautifulSoup(head, 'lxml')
    body_soup = BeautifulSoup(body, 'lxml')
    links, embeds = parse.find_head_links_soup(head_soup)
    lbody, ebody = parse.find_body_links_soup(body_soup)
    links.update(lbody)
    embeds.update(ebody)
    assert len(links) == 3
    assert len(embeds) == 2
    assert 'foo3.html ' in links  # this space will disapper in urls.URL()
    assert 'foo.gif' in embeds

test_css = '''
@import url('foo1.css')
url(images/foo2.png)
url( images/foo3.png )
'''


def test_css_parser():
    links, embeds = parse.find_css_links_re(test_css)
    assert len(links) == 0
    assert len(embeds) == 3
    assert 'images/foo3.png' in embeds


def test_regex_out_comments():
    t = 'Hello <!-- foo --> world!'
    assert parse.regex_out_comments(t) == 'Hello  world!'


def test_regex_out_some_scripts():
    t = '<script>foo</script> bar'
    assert parse.regex_out_some_scripts(t) == ' bar'


def test_regex_out_all_script():
    t = '<script>foo</script> bar <script type="baz">barf</script> '
    assert parse.regex_out_all_scripts(t) == ' bar  '
