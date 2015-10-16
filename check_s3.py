#!/usr/bin/env python
import argparse
import time
import sys
import traceback
import os
import tempfile
import boto.s3 as s3
from boto.s3.connection import Location
from check_lib import NagiosReturn,is_within_range
try:
    import cPickle as pickle
except:
    import pickle

class PickleFile(object):

    def __init__(self,bucket_name):
        self.name=os.path.join(tempfile.gettempdir(),'check_s3-{}'.format(bucket_name))
        print "file name: {}".format(self.name)

    def read(self):
        try:
            with open(self.name,'r') as f:
                return pickle.load(f)
        except IOError:
            return None

    def write(self,data):
        with open(self.name,'w') as f:
            pickle.dump(data,f)


class S3Check(object):
    OK=0
    WARNING=1
    CRITICAL=2

    def __init__(self):
        self.args=None
        self.time2connect=-1
        self.mode=None
        self.con=None
        self.bucket=None


    def parse_args(self):   
        regions=[getattr(Location,r) for r in dir(Location) if r[0].isupper()]

        p=argparse.ArgumentParser(prog='check_s3.py')
        p.add_argument('-w','--warning',dest='warning',action='store',help="Nagios warning level (required)", default=None)
        p.add_argument('-c','--critical',dest='critical',action='store',help="Nagios critical level (required)",default=None)
        p.add_argument('--key_id',dest='key_id',action='store',help='AWS Secret Key ID',default=None)
        p.add_argument('--secret',dest='secret',action='store',help='AWS Secret Access Key',default=None)
        p.add_argument('--region', choices=regions, help='Region to work with',default='us-west-2')
        p.add_argument('--bucket',dest='bucket',action='store',help='The bucket to check (required)')
        p.add_argument('--connect', action='store_true', help='Check the time to connect (default)')
        p.add_argument('--daily_growth', action='store_true', help='Check the daily growth in MB')
        p.add_argument('--size', action='store_true', help='Check the bucket size in GB')

        self.args=p.parse_args()

        # check for required args
        required=('warning','critical','bucket')
        for r in required:
            if not hasattr(self.args,r) or getattr(self.args,r) is None:
                p.error('The argument --{} is required'.format(r))

        # make sure we have a mode
        for m in ('connect','daily_growth','size'):
            if hasattr(self.args,m) and getattr(self.args,m):
                self.mode=m
                break
        else:
            self.mode='connect'

        return self.args


    def do_check(self):

        try:
            {   'connect': self.check_connect,
                'size': self.check_size,
                'daily_growth': self.check_growth
            } [self.mode]()

        except KeyError:
            raise Exception('Mode {} is not implemented yet'.format(self.mode))    


    def get_status_code(self,value):
        if is_within_range(self.args.critical, value):
            return S3Check.CRITICAL

        elif is_within_range(self.args.warning, value):
            return S3Check.WARNING
        
        else:
            return S3Check.OK

    def get_code_string(self,code):
        return ('OK:','WARNING:','CRITICAL:')[code]


    def check_growth(self):
        (size,count)=self.get_bucket_size()
        pf=PickleFile(self.args.bucket)
        data=pf.read()
        now=time.time()
        print "read: {}".format(data)
        try:
            # days_since_last_sample=(now-data['time'])/(24.0*60.0*60.0)
            days_since_last_sample=(now-data['time'])/30.0
            if days_since_last_sample>=1.0:
                size_rate=(size-data['size'])/days_since_last_sample
                count_rate=(count-data['count'])/days_since_last_sample
                print "Last sample more than a day ago"
            else:
                size_rate=data['size_rate']
                count_rate=data['count_rate']
                print "Last sample is less than a day old"
            data={
                'time': now,
                'size': size,
                'count': count,
                'size_rate': size_rate,
                'count_rate': count_rate
            }
            print "New sample: {}".format(data)
        except (KeyError,TypeError):
            data={
                'time': now,
                'size': size,
                'count': count,
                'size_rate': 0,
                'count_rate': 0
            }
            print "no pf yet: Initialized to {}".format(data)
        pf.write(data)

        size_rate=data['size_rate']*1024.0
        count_rate=data['count_rate']
        code=self.get_status_code(size_rate)

        message=[ self.get_code_string(code) ]
        message.append('daily_growth={:5.3f} MB'.format(size_rate))
        message.append('|')
        message.append('daily_size_growth={:5.3f};;{};{}'.format(size_rate,self.args.critical,self.args.warning))
        message.append('daily_count_growth={};;;'.format(count_rate))

        raise NagiosReturn(' '.join(message), code)

    def check_size(self):
        (size,count)=self.get_bucket_size()

        code=self.get_status_code(size)
        message=[ self.get_code_string(code) ]
        message.append('bucket size={:5.3f} GB'.format(size))
        message.append('|')
        message.append('size={:5.3f};;{};{}'.format(size,self.args.critical,self.args.warning))
        message.append('count={};;;'.format(count))

        raise NagiosReturn(' '.join(message),code)


    def check_connect(self):
        code=self.get_status_code(self.time2connect)
        message=[self.get_code_string(code)]
        message.append('connect time={:4.3f} sec'.format(self.time2connect))
        message.append('|')
        message.append('connect_time={:4.3f};;{};{}'.format(self.time2connect,self.args.critical,self.args.warning))

        raise NagiosReturn(' '.join(message), code)


    def connect(self):
        time2connect=time.time()
        self.con=s3.connect_to_region(
            self.args.region, 
            aws_access_key_id=self.args.key_id,
            aws_secret_access_key=self.args.secret,
            is_secure=True
        )
        self.bucket=self.con.get_bucket(self.args.bucket)
        self.time2connect=time.time()-time2connect


    def get_bucket_size(self):
        size=0
        count=0
        for key in self.bucket:
            size+=key.size
            count+=1
        size=size/(1024.0*1024.0*1024.0)
        return(size,count)

if __name__ == '__main__':
    try:
        check=S3Check()
        check.parse_args()
        check.connect()
        check.do_check()

        

    except NagiosReturn,e:
        print e.message
        sys.exit(e.code)

    except Exception,e:
        traceback.print_exc()
        sys.exit(3)

    