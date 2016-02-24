class wifi_data(object):
    def __init__(self,mac,t,sn):
        self.mac=mac
        self.t=t
        self.sn=sn

    def foo(self):
        self.sn=3
        print 'xxx'



def determine_type(wifi_data):
    wifi_data.foo()
    if wifi_data.t >=2 and wifi_data.sn>=2:
        return 'equipment'
    else:
        return 'customer'