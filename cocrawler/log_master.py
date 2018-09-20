import os


class LogMaster():

    _instances = {}  # keep all open file objects at class level

    def __init__(self,mode='a+',close_everytime=False):
        """

        :param mode:
        :param close_evertime: close file with every append, usefull when scripts possible can run into errors
        """
        self.close_everytime = close_everytime
        self.mode = mode


    def write(self, file_path, line, close_everytime=False):


        _file_path = str(file_path)
        line_to_write = line + '\n'

        if self.close_everytime or close_everytime:
            with open(_file_path,mode=self.mode) as f:
                f.write(line_to_write)

        else:
            if _file_path not in self.__class__._instances.keys():
                #init new file object and keep it open
                obj = open(_file_path,mode=self.mode)
                self.__class__._instances[_file_path] = obj

            else:
                obj = self.__class__._instances[_file_path]


            obj.write(line_to_write)


    @classmethod
    def close_all(cls):
        if len(cls._instances):
            for path,obj in cls._instances.items():
                obj.close()


