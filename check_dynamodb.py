#!/usr/bin/env python
import time
import boto.dynamodb2 as dynamodb2
from aws_lib import AWSCheck,AWSCheckParser,checkmode
from check_lib import NagiosReturn

class DynamoCheckParser(AWSCheckParser):

    def __init__(self,prog,mode_map):
        regions=[ ri.name for ri in dynamodb2.regions()]
        super(DynamoCheckParser,self).__init__(prog=prog,mode_map=mode_map,regions=regions,required=('table',))
        self.add_argument('--table',action='store',default=None,help='Name of the dynamodb table (requried)')


class DynamoCheck(AWSCheck):

    def connect(self):
        start=time.time()
        self.con=dynamodb2.connect_to_region(self.args.region,
            aws_access_key_id=self.args.key_id,
            aws_secret_access_key=self.args.secret,
            is_secure=True
        )
        self.time2connect=time.time()-start

    def parse_args(self):
        return super(DynamoCheck,self).parse_args(prog='check_dynamodb.py',Parser=DynamoCheckParser)

    @checkmode('size','Checks the size of the table')
    def check_size(self):
        size=self.con.describe_table(self.args.table)['Table']['TableSizeBytes']
        size=size/(1024.0*1024.0)

        code=self.get_status_code(size)
        message=[self.get_code_string(code)]
        message.append('table size={:4.3f} MB'.format(size))
        message.append('|')
        message.append('table_size={:4.3f};{};{};;'.format(size,self.args.warning,self.args.critical))

        raise NagiosReturn(' '.join(message), code)



if __name__ == '__main__':

    ac=DynamoCheck()
    ac.do()
