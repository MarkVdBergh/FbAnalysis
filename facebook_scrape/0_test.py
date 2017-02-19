from facebook_scrape.settings import DB as db

x=db
print x


class A():
    class_v=0
    class_vv=0
    def __init__(self):
        A.class_v=1
        self.inst_v=1
    def m(self):
        A.class_vv=2
        self.inst_v=2
a=A()
print a.class_v
print a.inst_v
a.m()
print a.class_v
print a.inst_v
b=A()
print b.class_v
print b.inst_v



