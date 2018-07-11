from tqdm import tqdm

class CounterBar():
    def __init__(self,ls=None,mininterval=0.1):
        """
        :param ls:
        :param mininterval: in seconds
        """
        if ls:
            if isinstance(ls,list):
                self.ls_max = len(ls)
            if isinstance(ls,int):
                self.ls_max = ls
            self.pbar = tqdm(total=self.ls_max,leave=False,mininterval=mininterval)
        self.current = 0
        self.mininterval = mininterval


    def init(self,ls,name=None):
        self.name = name
        # deferred initialization
        if isinstance(ls,list):
            self.ls_max = len(ls)
        if isinstance(ls,int):
            self.ls_max = ls
        self.pbar = tqdm(total=self.ls_max,leave=False,mininterval=self.mininterval)

    def count(self):
        if not getattr(self,'ls_max',None):
            raise AttributeError('--> Init counter first !!!')
        self.pbar.update(1)
