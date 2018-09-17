import os


class LogMaster():

    _instances = {}  # keep all open file objects at class level

    def __init__(self,mode='a+'):
        self.mode = mode


    def write(self,file_path,line):

        if file_path not in self.__class__._instances.keys():
            #init new file object and keep it open
            obj = open(file_path,mode=self.mode)
            self.__class__._instances[file_path] = obj

        else:
            obj = self.__class__._instances[file_path]


        obj.write(line + '\n')

    @classmethod
    def close_all(cls):
        if len(cls._instances):
            for path,obj in cls._instances.items():
                obj.close()


