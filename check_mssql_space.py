#!/usr/bin/env python
#################### check_mssql_database.py ############################
# Date : 14 Oct 2015
# Author  : Chris Ampenberger (campenberger at lexington-solutions.com)
# Help : campenberger at lexington-solutions.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
# 
# Change Log:
# 2015-10-14 Initial version to check free disk space on a SQL Server, 
#   usefule for RDS instance
#########################################################################

import sys
from check_lib import CheckOptions,connect_db, return_nagios, NagiosReturn,MSSQLQuery,is_within_range
from collections import namedtuple

def parse_args():
    modes={
        'space':    {
            'help'  : 'Disk space',
            'label' : 'disk_space',
        },
        'time2connect': {
            'help'  : 'Connect time only',
            'label' : 'connect time'
        }
    }
    p=CheckOptions(modes, usage = "usage: %prog -H hostname -U user -P password --space")
    return p.parse_args()

Row=namedtuple('Row',['drive','name','size','free'])
Result=namedtuple('Result',['drive','name','size','free','used_percent','status_code'])

class SpaceQuery(MSSQLQuery):
    QUERY='''
        SELECT DISTINCT
          vs.volume_mount_point AS [Drive],
          vs.logical_volume_name AS [Drive Name],
          vs.total_bytes/1024/1024 AS [Drive Size MB],
          vs.available_bytes/1024/1024 AS [Drive Free Space MB]
        FROM sys.master_files AS f
        CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
        ORDER BY vs.volume_mount_point
    '''    

    OK=0
    WARNING=1
    CRITICAL=2

    def __init__(self,*args,**kwargs):
        super(SpaceQuery,self).__init__(SpaceQuery.QUERY,*args,**kwargs)

    def run_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.query)
        row=cur.fetchone()
        self.query_result=[]
        while row:
            self.query_result.append(Row(*row))
            row=cur.fetchone()
        cur.close()

        
    
    def calculate_result(self):
        def _calc_result_row(row):
            used_percent=100.0*(row.size-row.free)/row.size
            if is_within_range(self.options.critical,used_percent):
                status_code=SpaceQuery.CRITICAL

            elif is_within_range(self.options.warning,used_percent):
                status_code=SpaceQuery.WARNING
            
            else:
                status_code=SpaceQuery.OK

            res=Result(
                    row.drive,
                    row.name,
                    row.size,
                    row.free,
                    used_percent,
                    status_code
                )
            return res
    
        self.results=map(_calc_result_row, self.query_result)


    def finish(self):
        if any(map(lambda x: x.status_code==SpaceQuery.CRITICAL, self.results)):
            status_code=SpaceQuery.CRITICAL
        elif any(map(lambda x: x.status_code==SpaceQuery.WARNING, self.results)):
            status_code=SpaceQuery.WARNING
        else:
            status_code=SpaceQuery.OK

        critical=self.options.critical if self.options.critical is not None else ""
        warning=self.options.warning if self.options.warning is not None else ""

        text=[('OK:','WARNING:', 'CRITICAL:')[status_code]]

        def _format_text(r):
            text.append("{} ({}): {:4.1f}% used ({} of {} MB);".format(r.drive, r.name, r.used_percent, r.size-r.free, r.size))

        def _format_spec(r):
            text.append("{} used %={:3.1f};;{};{}".format(r.drive,r.used_percent,critical,warning))
            text.append("{} free MB={};;;".format(r.drive,r.free))
            text.append("{} szie MB={};;;".format(r.drive,r.size))

        map(_format_text,self.results)
        text.append('|')
        map(_format_spec,self.results)

        raise NagiosReturn(" ".join(text), status_code)


if __name__ == '__main__':
    options=parse_args()
    (mssql, total, host)=connect_db(options)

    try:
        if not options.mode or options.mode == 'time2connect':
            return_nagios(
                options,
                stdout='Time to connect was %ss',
                label='time',
                unit='s',
                result=total
            )

        elif options.mode=='space':
            qry=SpaceQuery(options,label='disk_space',unit='MB')
            qry.do(mssql)

    except NagiosReturn,e:
        print e.message
        sys.exit(e.code)