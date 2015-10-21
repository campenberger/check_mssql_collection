import argparse
import traceback
import sys
from collections import namedtuple
from abc import ABCMeta,abstractmethod
from check_lib import NagiosReturn,is_within_range

Mode=namedtuple('Mode',('name','fct','help','default'))
mode_map={}

class AWSCheckParser(argparse.ArgumentParser):

    def __init__(self,prog,mode_map,regions,required=None):
        super(AWSCheckParser,self).__init__(prog=prog)

        self.required=('warning','critical') + () if required is None else required
        self.modes=mode_map.keys()

        self.add_argument('-w','--warning',dest='warning',action='store',help="Nagios warning level (required)", default=None)
        self.add_argument('-c','--critical',dest='critical',action='store',help="Nagios critical level (required)",default=None)
        self.add_argument('--key_id',dest='key_id',action='store',help='AWS Secret Key ID',default=None)
        self.add_argument('--secret',dest='secret',action='store',help='AWS Secret Access Key',default=None)
        self.add_argument('--region', choices=regions, help='Region to work with',default='us-west-2')

        for mode in mode_map.itervalues():
            self.add_argument('--'+mode.name, action='store_true', help=mode.help)
            if mode.default:
                self.default_mode=mode.name



    def parse_args(self):
        args=super(AWSCheckParser,self).parse_args()

        # check for required args
        for r in self.required:
            if not hasattr(args,r) or getattr(args,r) is None:
                self.error('The argument --{} is required'.format(r))

        # make sure we have a mode
        for m in self.modes if self.modes else []:
            if hasattr(args,m) and getattr(args,m):
                mode=m
                break
        else:
            mode=self.default_mode

        return (args,mode)


def checkmode(mode,help_str,default=False):

    def wrapper(f,*args,**kwargs):
        global mode_map
        mode_map[mode]=Mode(mode,f,help_str,default)

        def wrapped(*args,**kwargs):
            return f(*args,**kwargs)

        return wrapped

    return wrapper


class AWSCheck(object):
    __metaclass__=ABCMeta

    OK=0
    WARNING=1
    CRITICAL=2

    def __init__(self):
        self.args=None
        self.time2connect=-1
        self.mode=None
        self.con=None

    def parse_args(self,prog=None,Parser=AWSCheckParser):
        global mode_map
        p=Parser(prog=prog,mode_map=mode_map)
        (self.args, self.mode)=p.parse_args()
        return (self.args, self.mode)


    @abstractmethod
    def connect(self):
        pass

    @checkmode('connect','Check the time to connect (default)',default=True)
    def check_connect(self):
        code=self.get_status_code(self.time2connect)
        message=[self.get_code_string(code)]
        message.append('connect time={:4.3f} sec'.format(self.time2connect))
        message.append('|')
        message.append('connect_time={:4.3f};{};{};;'.format(self.time2connect,self.args.warning,self.args.critical))

        raise NagiosReturn(' '.join(message), code)


    def get_status_code(self,value):
        if is_within_range(self.args.critical, value):
            return AWSCheck.CRITICAL

        elif is_within_range(self.args.warning, value):
            return AWSCheck.WARNING
        
        else:
            return AWSCheck.OK


    def get_code_string(self,code):
        return ('OK:','WARNING:','CRITICAL:')[code]


    def do(self):
        try:
            self.parse_args()
            self.connect()
            try:
                global mode_map
                f=mode_map[self.mode]
            except KeyError:
                raise Exception('Mode {} is not implemented yet'.format(self.mode))

            f.fct(self)

        except NagiosReturn,e:
            print e.message
            sys.exit(e.code)

        except Exception,e:
            traceback.print_exc()
            sys.exit(3)