from .urls import URL
from weakref import WeakKeyDictionary
from inspect import isfunction


class ValidatorError(Exception):pass


class Validator():
    _types = [dict,list,str,bool,URL]

    def __init__(self,val):
        self.val = val

    def istype(self,obj):
        return any([x for x in self._types if x==obj])

    def validate(self,value):
        if not isinstance(self.val,list):
            vls = [self.val,]
        else:
            vls = self.val

        for validator in vls:

            if self.istype(validator):
                if not isinstance(value,validator):
                    raise ValidatorError(' value "{0}" is not an instance of [{1}]'.format(value,self.val))

            elif isfunction(validator):
                try:
                    validator(value)
                except Exception as ex:
                    raise ValidatorError(' validator function returned error for value:{0} : {1}'.format(value,ex))
            else:
                pass

class SessionData():
    def __init__(self,data=None,validator=None):
        self.init_data = data
        self._data = WeakKeyDictionary()
        self.validator = Validator(validator)


    def __set__(self, instance, value):

        if value is not None:
            self.validator.validate(value)

        self._data[instance] = value

    def __get__(self, instance, owner):
        return self._data.get(instance)



class Req():
    '''
    class attributes directly change ClientSession
    '''
    url = SessionData(validator=URL)
    post = SessionData(validator=dict)
    cookies = SessionData(validator=dict)
    multipart_post = SessionData(validator=bool)
    headers = SessionData(validator=dict)

    def __init__(self, url, post=None, cookies=None, multipart_post=False, headers=None):

        self.url = URL(url)

        self.post = post
        self.cookies = cookies
        self.multipart_post = multipart_post
        self.headers = headers

    @property
    def method(self):
        if self.post is not None:
            return 'POST'
        else:
            return 'GET'

    def update_headers(self,val):
        if isinstance(val,dict):
            self.headers.update(val)
        else:
            raise ValueError('--> Value must be dict')

    def set_useragent(self,val):
        self.update_headers({'User-Agent':val})

    def reset(self):
        self.headers = None
        self.cookies = None
        self.post = None
        self.multipart_post = False



    @classmethod
    def options(cls):
        # all allowed option to pass to session
        datas = [key for key,val in cls.__dict__.items() if isinstance(val,SessionData)]
        return datas

    def __setattr__(self, key, value):
        if key not in self.__class__.options():
            raise KeyError('--> "{0}" option is not allowed'.format(key))


        try:
            # re-catch validation error to get the Key that caused the error
            super().__setattr__(key,value)

        except ValidatorError as ex:
            raise ValidatorError('--> Validation error for [ {0} ] : {1}'.format(key,ex))
