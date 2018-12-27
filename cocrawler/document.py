import defusedxml.lxml
from lxml.html import HTMLParser,HtmlElement
from selection import XpathSelector
from six import BytesIO, StringIO
from six.moves.urllib.parse import urlsplit, parse_qs, urljoin
from weblib.http import smart_urlencode
import json
import os
from cocrawler import content
from cocrawler import stats


from .req import Req

'''
https://github.com/rushter/selectolax - faster 20 times then lxml
'''



class DocError(Exception):
    pass

class DataNotFound(DocError):
    pass

class FormExtension(object):
    __slots__ = ()

    def choose_form(self, number=None, id=None, name=None, xpath=None):
        """
        Set the default form.

        :param number: number of form (starting from zero)
        :param id: value of "id" attribute
        :param name: value of "name" attribute
        :param xpath: XPath query
        :raises: :class:`DataNotFound` if form not found
        :raises: :class:`GrabMisuseError`
            if method is called without parameters

        Selected form will be available via `form` attribute of `Grab`
        instance. All form methods will work with default form.

        Examples::

            # Select second form
            g.choose_form(1)

            # Select by id
            g.choose_form(id="register")

            # Select by name
            g.choose_form(name="signup")

            # Select by xpath
            g.choose_form(xpath='//form[contains(@action, "/submit")]')
        """

        if id is not None:
            try:
                self._lxml_form = self.select('//form[@id="%s"]' % id).node()
            except IndexError:
                raise DataNotFound("There is no form with id: %s" % id)
        elif name is not None:
            try:
                self._lxml_form = self.select(
                    '//form[@name="%s"]' % name).node()
            except IndexError:
                raise DataNotFound('There is no form with name: %s' % name)
        elif number is not None:
            try:
                self._lxml_form = self.tree.forms[number]
            except IndexError:
                raise DataNotFound('There is no form with number: %s' % number)
        elif xpath is not None:
            try:
                self._lxml_form = self.select(xpath).node()
            except IndexError:
                raise DataNotFound(
                    'Could not find form with xpath: %s' % xpath)
        else:
            raise ValueError('choose_form methods requires one of '
                                  '[number, id, name, xpath] arguments')

    @property
    def form(self):
        """
        This attribute points to default form.

        If form was not selected manually then select the form
        which has the biggest number of input elements.

        The form value is just an `lxml.html` form element.

        Example::

            g.go('some URL')
            # Choose form automatically
            print g.form

            # And now choose form manually
            g.choose_form(1)
            print g.form
        """

        if self._lxml_form is None:
            self.parse()

            forms = [(idx, len(list(x.fields)))
                     for idx, x in enumerate(self.tree.forms)]
            if len(forms):
                idx = sorted(forms, key=lambda x: x[1], reverse=True)[0][0]
                self.choose_form(idx)
            else:
                raise DataNotFound('Response does not contains any form')
        return self._lxml_form

    def set_input(self, name, value):
        """
        Set the value of form element by its `name` attribute.

        :param name: name of element
        :param value: value which should be set to element

        To check/uncheck the checkbox pass boolean value.

        Example::

            g.set_input('sex', 'male')

            # Check the checkbox
            g.set_input('accept', True)
        """

        if self._lxml_form is None:
            self.choose_form_by_element('.//*[@name="%s"]' % name)
        elem = self.form.inputs[name]

        processed = False
        if getattr(elem, 'type', None) == 'checkbox':
            if isinstance(value, bool):
                elem.checked = value
                processed = True

        if not processed:
            # We need to remember original values of file fields
            # Because lxml will convert UploadContent/UploadFile object to
            # string
            if getattr(elem, 'type', '').lower() == 'file':
                self._file_fields[name] = value
                elem.value = ''
            else:
                elem.value = value

    def set_input_by_id(self, _id, value):
        """
        Set the value of form element by its `id` attribute.

        :param _id: id of element
        :param value: value which should be set to element
        """

        xpath = './/*[@id="%s"]' % _id
        if self._lxml_form is None:
            self.choose_form_by_element(xpath)
        sel = XpathSelector(self.form)
        elem = sel.select(xpath).node()
        return self.set_input(elem.get('name'), value)

    def set_input_by_number(self, number, value):
        """
        Set the value of form element by its number in the form

        :param number: number of element
        :param value: value which should be set to element
        """

        sel = XpathSelector(self.form)
        elem = sel.select('.//input[@type="text"]')[number].node()
        return self.set_input(elem.get('name'), value)

    def set_input_by_xpath(self, xpath, value):
        """
        Set the value of form element by xpath

        :param xpath: xpath path
        :param value: value which should be set to element
        """

        elem = self.select(xpath).node()

        if self._lxml_form is None:
            # Explicitly set the default form
            # which contains found element
            parent = elem
            while True:
                parent = parent.getparent()
                if parent.tag == 'form':
                    self._lxml_form = parent
                    break

        return self.set_input(elem.get('name'), value)

    # TODO:
    # Remove set_input_by_id
    # Remove set_input_by_number
    # New method: set_input_by(id=None, number=None, xpath=None)

    def get_req(self, submit_name=None,url=None, extra_post=None, remove_from_post=None):
        """
        Submit default form.

        :param submit_name: name of button which should be "clicked" to
            submit form
        :param make_request: if `False` then grab instance will be
            configured with form post data but request will not be
            performed
        :param url: explicitly specify form action url
        :param extra_post: (dict or list of pairs) additional form data which
            will override data automatically extracted from the form.
        :param remove_from_post: list of keys to remove from the submitted data

        Following input elements are automatically processed:

        * input[type="hidden"] - default value
        * select: value of last option
        * radio - ???
        * checkbox - ???

        Multipart forms are correctly recognized by grab library.

        Example::

            # Assume that we going to some page with some form
            g.go('some url')
            # Fill some fields
            g.set_input('username', 'bob')
            g.set_input('pwd', '123')
            # Submit the form
            g.submit()

            # or we can just fill the form
            # and do manual submission
            g.set_input('foo', 'bar')
            g.submit(make_request=False)
            g.request()

            # for multipart forms we can specify files
            from grab import UploadFile
            g.set_input('img', UploadFile('/path/to/image.png'))
            g.submit()
        """

        # TODO: add .x and .y items
        # if submit element is image

        _multipart = False

        post = self.form_fields()

        # Build list of submit buttons which have a name
        submit_controls = {}
        for elem in self.form.inputs:
            if (elem.tag == 'input' and elem.type == 'submit' and
                elem.get('name') is not None):
                submit_controls[elem.name] = elem

        # All this code need only for one reason:
        # to not send multiple submit keys in form data
        # in real life only this key is submitted whose button
        # was pressed
        if len(submit_controls):
            # If name of submit control is not given then
            # use the name of first submit control
            if submit_name is None or submit_name not in submit_controls:
                controls = sorted(submit_controls.values(),
                                  key=lambda x: x.name)
                submit_name = controls[0].name

            # Form data should contain only one submit control
            for name in submit_controls:
                if name != submit_name:
                    if name in post:
                        del post[name]

        if url:
            action_url = urljoin(self.url, url)
        else:
            action_url = urljoin(self.url, self.form.action)

        # Values from `extra_post` should override values in form
        # `extra_post` allows multiple value of one key

        # Process saved values of file fields
        if self.form.method == 'POST':
            if 'multipart' in self.form.get('enctype', ''):
                for key, obj in self._file_fields.items():
                    post[key] = obj

        post_items = list(post.items())
        del post

        if extra_post:
            if isinstance(extra_post, dict):
                extra_post_items = extra_post.items()
            else:
                extra_post_items = extra_post

            # Drop existing post items with such key
            keys_to_drop = set([x for x, y in extra_post_items])
            for key in keys_to_drop:
                post_items = [(x, y) for x, y in post_items if x != key]

            for key, value in extra_post_items:
                post_items.append((key, value))

        if remove_from_post:
            post_items = [(x, y) for x, y in post_items
                          if x not in remove_from_post]

        if self.form.method == 'POST':
            if 'multipart' in self.form.get('enctype', ''):
                #self.grab.setup(multipart_post=post_items)
                _multipart = True


        else:
            action_url = action_url.split('?')[0] + '?' + smart_urlencode(post_items)


        req = Req(url=action_url)
        req.multipart_post= _multipart
        req.post = dict(post_items)

        return req

    def form_fields(self):
        """
        Return fields of default form.

        Fill some fields with reasonable values.
        """

        fields = dict(self.form.fields)
        for elem in self.form.inputs:
            # Ignore elements without name
            if not elem.get('name'):
                continue

            # Do not submit disabled fields
            # http://www.w3.org/TR/html4/interact/forms.html#h-17.12
            if elem.get('disabled'):
                if elem.name in fields:
                    del fields[elem.name]

            elif elem.tag == 'select':
                if fields[elem.name] is None:
                    if len(elem.value_options):
                        fields[elem.name] = elem.value_options[0]

            elif getattr(elem, 'type', None) == 'radio':
                if fields[elem.name] is None:
                    fields[elem.name] = elem.get('value')

            elif getattr(elem, 'type', None) == 'checkbox':
                if not elem.checked:
                    if elem.name is not None:
                        if elem.name in fields:
                            del fields[elem.name]

        return fields

    def choose_form_by_element(self, xpath):
        elem = self.select(xpath).node()
        while elem is not None:
            if elem.tag == 'form':
                self._lxml_form = elem
                return
            else:
                elem = elem.getparent()
        self._lxml_form = None


class Document(FormExtension):
    def __init__(self,html=None,url=None):
        self._html = html
        self.url = url
        self.content_data = None # tuple (content_type, content_encoding, charset, charset_used)
                                #charset = what we detect using html,
                                #charset_used = what we eventually used to decode

        self.burner = None # result of the burner work = (links, embeds, base)
        self._etree = None
        self._fetcher = None # fetcher complete response object, see property
        self.status = None # (int or last_exception) status taken from fetcher response object for quick access
        self._lxml_form = None

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
        if self.tree is None:
            self.parse()
        return XpathSelector(self._etree).select(*args, **kwargs)


    def _get_etree(self):
        return self._etree

    def _set_etree(self, obj):
        if isinstance(obj,HtmlElement):
            self._etree = obj
        else:
            raise ValueError('--> object is not HtmlElement')


    tree = property(_get_etree, _set_etree)


    def _get_html(self):
        return self._html

    def _set_html(self, html):
        self._html = html

    # exists only for certain type of content : text/html, json etc
    html = property(_get_html, _set_html)


    def _get_fetcher(self):
        return self._fetcher

    def _set_fetcher(self, fetcher):
        self._fetcher = fetcher

    #fetcher complete response object
    fetcher = property(_get_fetcher, _set_fetcher)


    @property
    def headers(self):
        return self.fetcher.response.raw_headers

    def save(self, path):
        """
        auto decompress body
        Save response body to file.
        """

        path_dir = os.path.split(path)[0]
        if not os.path.exists(path_dir):
            try:
                os.makedirs(path_dir)
            except OSError:
                pass

        body_bytes = self.fetcher.body_bytes
        if self.content_data:
            content_type, content_encoding, charset, charset_used = self.content_data

            if content_encoding and content_encoding != 'identity':
                with stats.record_burn('response body decompress'):
                    body_bytes = content.decompress(self.fetcher.body_bytes, content_encoding)


            #
            # charset, detect = content.my_get_charset(charset, body_bytes)
            #
            # body, charset_used = content.my_decode(body_bytes, charset, detect)


        with open(path, 'wb') as out:
            out.write(body_bytes if body_bytes is not None
                      else b'')


