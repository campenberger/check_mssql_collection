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
from aws_lib import AWSCheck,AWSCheckParser,checkmode
try:
    import cPickle as pickle
except:
    import pickle

class PickleFile(object):

    def __init__(self,bucket_name):
        self.name=os.path.join(tempfile.gettempdir(),'check_s3-{}'.format(bucket_name))

    def read(self):
        try:
            with open(self.name,'r') as f:
                return pickle.load(f)
        except IOError:
            return None

    def write(self,data):
        with open(self.name,'w') as f:
            pickle.dump(data,f)

class S3CheckParser(AWSCheckParser):

    def __init__(self,prog,mode_map):
        regions=[getattr(Location,r) for r in dir(Location) if r[0].isupper()]
        super(S3CheckParser,self).__init__(prog=prog,mode_map=mode_map,regions=regions,required=('bucket',))
        self.add_argument('--bucket',action='store',default=None,help='The bucket to check (required)')


class S3Check(AWSCheck):

    def __init__(self):
        self.bucket=None

    
    def parse_args(self):
        return super(S3Check,self).parse_args(prog='check_s3.py',Parser=S3CheckParser)

    
    @checkmode('daily_growth','Check the daily growth in MB')
    def check_growth(self):
        (size,count)=self.get_bucket_size()
        pf=PickleFile(self.args.bucket)
        data=pf.read()
        now=time.time()
        try:
            days_since_last_sample=(now-data['time'])/(24.0*60.0*60.0)
            if days_since_last_sample>=1.0:
                size_rate=(size-data['size'])/days_since_last_sample
                count_rate=(count-data['count'])/days_since_last_sample
            else:
                size_rate=data['size_rate']
                count_rate=data['count_rate']
            data={
                'time': now,
                'size': size,
                'count': count,
                'size_rate': size_rate,
                'count_rate': count_rate
            }
        except (KeyError,TypeError):
            data={
                'time': now,
                'size': size,
                'count': count,
                'size_rate': 0,
                'count_rate': 0
            }
        pf.write(data)

        size_rate=data['size_rate']*1024.0
        count_rate=data['count_rate']
        code=self.get_status_code(size_rate)

        message=[ self.get_code_string(code) ]
        message.append('daily_growth={:5.3f} MB'.format(size_rate))
        message.append('|')
        message.append('daily_size_growth={:5.3f};{};{};;'.format(size_rate,self.args.warning,self.args.critical))
        message.append('daily_count_growth={};;;'.format(count_rate))

        raise NagiosReturn(' '.join(message), code)


    @checkmode('size','Check the bucket size in GB')
    def check_size(self):
        (size,count)=self.get_bucket_size()

        code=self.get_status_code(size)
        message=[ self.get_code_string(code) ]
        message.append('bucket size={:5.3f} GB'.format(size))
        message.append('|')
        message.append('size={:5.3f};{};{};;'.format(size,self.args.warning,self.args.critical))
        message.append('count={};;;'.format(count))

        raise NagiosReturn(' '.join(message),code)


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
    check=S3Check()
    check.do()
