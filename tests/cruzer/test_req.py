#from cocrawler.req import Req
from weakref import WeakKeyDictionary
from inspect import isfunction
import types

class ValidatorError(Exception):pass



class Validator():
    _types = [dict,list,str]

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
                    raise ValidatorError('--> value [{0}] is not an instance of [{1}]'.format(value,self.val))

            elif isfunction(validator):
                try:
                    validator(value)
                except Exception as ex:
                    raise ValidatorError('--> validator function returned error for value:{0} : {1}'.format(value,ex))
            else:
                pass

class SessionData():
    def __init__(self,data=None,validator=None):
        self.init_data = data
        self._data = WeakKeyDictionary()
        self.validator = Validator(validator)


    def __set__(self, instance, value):

        self.validator.validate(value)

        self._data[instance] = value

    def __get__(self, instance, owner):
        return self._data.get(instance)






class Req():
    post = SessionData(validator=str)

    def __init__(self,post):
        self.post = post


    @classmethod
    def options(cls):
        # all allowed option to pass to session
        datas = [key for key,val in cls.__dict__.items() if isinstance(val,SessionData)]
        return datas

    def __setattr__(self, key, value):
        if key not in self.__class__.options():
            raise KeyError('--> "{0}" option is not allowed'.format(key))

        super().__setattr__(key,value)



def main():
    req = Req('http://tut.by')
    req2 = Req('http://google.by')

    req.post = 'dddd'
    print(vars(req))
    print(req.post)



def misc():
    from cocrawler.req import Req

    req = Req('http://tut.by')
    req.url = 'http://google.com'
    print(req.url)
if __name__ == '__main__':
    #main()
    misc()
