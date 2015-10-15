import tempfile
import time
import pymssql

try:
    import cPickle as pickle
except:
    import pickle

from optparse import OptionParser, OptionGroup

class NagiosReturn(Exception):
    
    def __init__(self, message, code):
        self.message = message
        self.code = code


def is_within_range(nagstring, value):
    if not nagstring:
        return False
    import re
    import operator
    first_float = r'(?P<first>(-?[0-9]+(\.[0-9]+)?))'
    second_float= r'(?P<second>(-?[0-9]+(\.[0-9]+)?))'
    actions = [ (r'^%s$' % first_float,lambda y: (value > float(y.group('first'))) or (value < 0)),
                (r'^%s:$' % first_float,lambda y: value < float(y.group('first'))),
                (r'^~:%s$' % first_float,lambda y: value > float(y.group('first'))),
                (r'^%s:%s$' % (first_float,second_float), lambda y: (value < float(y.group('first'))) or (value > float(y.group('second')))),
                (r'^@%s:%s$' % (first_float,second_float), lambda y: not((value < float(y.group('first'))) or (value > float(y.group('second')))))]
    for regstr,func in actions:
        res = re.match(regstr,nagstring)
        if res: 
            return func(res)
    raise Exception('Improper warning/critical format.')


def return_nagios(options, stdout='', result='', unit='', label=''):
    if is_within_range(options.critical, result):
        prefix = 'CRITICAL: '
        code = 2
    elif is_within_range(options.warning, result):
        prefix = 'WARNING: '
        code = 1
    else:
        prefix = 'OK: '
        code = 0
    strresult = str(result)
    try:
        stdout = stdout % (strresult)
    except TypeError, e:
        pass
    stdout = '%s%s|%s=%s%s;%s;%s;;' % (prefix, stdout, label, strresult, unit, options.warning or '', options.critical or '')
    raise NagiosReturn(stdout, code)


class MSSQLQuery(object):
    
    def __init__(self, query, options, label='', unit='', stdout='', host='', modifier=1, *args, **kwargs):
        if hasattr(options,'query_args'):
            self.query = query % options.query_args
        else:
            self.query = query
        self.label = label
        self.unit = unit
        self.stdout = stdout
        self.options = options
        self.host = host
        self.modifier = modifier
    
    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = cur.fetchone()[0]
    
    def finish(self):
        return_nagios(  self.options,
                        self.stdout,
                        self.result,
                        self.unit,
                        self.label )
    
    def calculate_result(self):
        self.result = float(self.query_result) * self.modifier
    
    def do(self, connection):
        self.run_on_connection(connection)
        self.calculate_result()
        self.finish()


class MSSQLDivideQuery(MSSQLQuery):
    
    def __init__(self, *args, **kwargs):
        super(MSSQLDivideQuery, self).__init__(*args, **kwargs)
    
    def calculate_result(self):
        if self.query_result[1] != 0:
            self.result = (float(self.query_result[0]) / self.query_result[1]) * self.modifier
        else:
            self.result = float(self.query_result[0]) * self.modifier
    
    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = [x[0] for x in cur.fetchall()]


class MSSQLDeltaQuery(MSSQLQuery):
    
    def make_pickle_name(self):
        tmpdir = tempfile.gettempdir()
        tmpname = hash(self.host + self.query)
        self.picklename = '%s/mssql-%s.tmp' % (tmpdir, tmpname)
    
    def calculate_result(self):
        self.make_pickle_name()
        
        try:
            tmpfile = open(self.picklename)
        except IOError:
            tmpfile = open(self.picklename, 'w')
            tmpfile.close()
            tmpfile = open(self.picklename)
        try:
            try:
                last_run = pickle.load(tmpfile)
            except EOFError, ValueError:
                last_run = { 'time' : None, 'value' : None }
        finally:
            tmpfile.close()
        
        if last_run['time']:
            old_time = last_run['time']
            new_time = time.time()
            old_val  = last_run['query_result']
            new_val  = self.query_result
            self.result = ((new_val - old_val) / (new_time - old_time)) * self.modifier
        else:
            self.result = None
        
        new_run = { 'time' : time.time(), 'query_result' : self.query_result }
        
        #~ Will throw IOError, leaving it to aquiesce
        tmpfile = open(self.picklename, 'w')
        pickle.dump(new_run, tmpfile)
        tmpfile.close()


class CheckOptions(OptionParser):

    def __init__(self,modes,*args,**kwargs):
        OptionParser.__init__(self,*args,**kwargs)
    
        required = OptionGroup(self, "Required Options")
        required.add_option('-H' , '--hostname', help='Specify MSSQL Server Address', default=None)
        required.add_option('-U' , '--user', help='Specify MSSQL User Name', default=None)
        required.add_option('-P' , '--password', help='Specify MSSQL Password', default=None)
        self.extend_required_group(required)
        self.add_option_group(required)
    
        connection = OptionGroup(self, "Optional Connection Information")
        connection.add_option('-I', '--instance', help='Specify instance', default=None)
        connection.add_option('-p', '--port', help='Specify port.', default=None)
        self.add_option_group(connection)
    
        nagios = OptionGroup(self, "Nagios Plugin Information")
        nagios.add_option('-w', '--warning', help='Specify warning range.', default=None)
        nagios.add_option('-c', '--critical', help='Specify critical range.', default=None)
        self.add_option_group(nagios)
    
        self.mode_group = OptionGroup(self, "Mode Options")
        for k, v in zip(modes.keys(), modes.values()):
            self.mode_group.add_option('--%s' % k, action="store_true", help=v.get('help'), default=False)
        self.add_option_group(self.mode_group)

    def extend_required_group(self,group):
        pass

    def parse_args(self):
        options, _ = OptionParser.parse_args(self)
    
        if not options.hostname:
            self.error('Hostname is a required option.')
        if not options.user:
            self.error('User is a required option.')
        if not options.password:
            self.error('Password is a required option.')
    
        if options.instance and options.port:
            self.error('Cannot specify both instance and port.')
    
        options.mode = None
        for arg in self.mode_group.option_list:
            if getattr(options, arg.dest) and options.mode:
                parser.error("Must choose one and only Mode Option.")
            elif getattr(options, arg.dest):
                options.mode = arg.dest
    
        return options


def connect_db(options):
    host = options.hostname
    if options.instance:
        host += "\\" + options.instance
    elif options.port:
        host += ":" + options.port

    table = options.table if hasattr(options,'table') else 'master'

    start = time.time()
    mssql = pymssql.connect(host = host, user = options.user, password = options.password, database=table)
    total = time.time() - start
    return mssql, total, host


def execute_query(mssql, options, modes, host=''):
    sql_query = modes[options.mode]
    sql_query['options'] = options
    sql_query['host'] = host
    query_type = sql_query.get('type')
    if query_type == 'delta':
        mssql_query = MSSQLDeltaQuery(**sql_query)
    elif query_type == 'divide':
        mssql_query = MSSQLDivideQuery(**sql_query)
    else:
        mssql_query = MSSQLQuery(**sql_query)
    mssql_query.do(mssql)

def run_tests(mssql, options, host, modes):
    failed = 0
    total  = 0
    del modes['time2connect']
    del modes['test']
    for mode in modes.keys():
        total += 1
        options.mode = mode
        try:
            execute_query(mssql, options, modes, host)
        except NagiosReturn:
            print "%s passed!" % mode
        except Exception, e:
            failed += 1
            print "%s failed with: %s" % (mode, e)
    print '%d/%d tests failed.' % (failed, total)


def main(modes, options):

    mssql, total, host = connect_db(options)
    if hasattr(options,'table'):
        options.query_args=(options.table,)

    try:
        if options.mode =='test':
            run_tests(mssql, options, host, modes)
            
        elif not options.mode or options.mode == 'time2connect':
            return_nagios(  options,
                            stdout='Time to connect was %ss',
                            label='time',
                            unit='s',
                            result=total )
                            
        else:
            execute_query(mssql, options, modes, host)

    except NagiosReturn,e:
        print e.message
        sys.exit(e.code)