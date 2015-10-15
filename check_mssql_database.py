#!/usr/bin/env python

########################################################################
# Date : Apr 4th, 2013
# Author  : Nicholas Scott ( scot0357 at gmail.com )
# Help : scot0357 at gmail.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
# TODO : Bug Testing, Feature Adding
# Changelog:
# 1.1.0 -   Fixed port bug allowing for non default ports | Thanks CBTSDon
#           Added mode error checking which caused non-graceful exit | Thanks mike from austria
# 1.2.0 -   Added ability to monitor instances
#           Added check to see if pymssql is installed
# 1.3.0 -   Added ability specify MSSQL instances
# 2.0.0 -   Complete Revamp/Rewrite based on the server version of this plugin
# 2.0.1 -   Fixed bug where temp file was named same as other for host and numbers
#           were coming back bogus.
########################################################################

from check_lib import CheckOptions, main

BASE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s' AND instance_name='%%s';"
DIVI_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name LIKE '%s%%%%' AND instance_name='%%s';"

MODES     = {
    
    'logcachehit'       : { 'help'      : 'Log Cache Hit Ratio',
                            'stdout'    : 'Log Cache Hit Ratio is %s%%',
                            'label'     : 'log_cache_hit_ratio',
                            'unit'      : '%',
                            'query'     : DIVI_QUERY % 'Log Cache Hit Ratio',
                            'type'      : 'divide',
                            'modifier'  : 100,
                            },
    
    'activetrans'       : { 'help'      : 'Active Transactions',
                            'stdout'    : 'Active Transactions is %s',
                            'label'     : 'log_file_usage',
                            'unit'      : '',
                            'query'     : BASE_QUERY % 'Active Transactions',
                            'type'      : 'standard',
                            },
    
    'logflushes'         : { 'help'      : 'Log Flushes Per Second',
                            'stdout'    : 'Log Flushes Per Second is %s/sec',
                            'label'     : 'log_flushes_per_sec',
                            'query'     : BASE_QUERY % 'Log Flushes/sec',
                            'type'      : 'delta'
                            },
    
    'logfileusage'      : { 'help'      : 'Log File Usage',
                            'stdout'    : 'Log File Usage is %s%%',
                            'label'     : 'log_file_usage',
                            'unit'      : '%',
                            'query'     : BASE_QUERY % 'Percent Log Used',
                            'type'      : 'standard',
                            },
    
    'transpec'          : { 'help'      : 'Transactions Per Second',
                            'stdout'    : 'Transactions Per Second is %s/sec',
                            'label'     : 'transactions_per_sec',
                            'query'     : BASE_QUERY % 'Transactions/sec',
                            'type'      : 'delta'
                            },
    
    'loggrowths'        : { 'help'      : 'Log Growths',
                            'stdout'    : 'Log Growths is %s',
                            'label'     : 'log_growths',
                            'query'     : BASE_QUERY % 'Log Growths',
                            'type'      : 'standard'
                            },
    'logshrinks'        : { 'help'      : 'Log Shrinks',
                            'stdout'    : 'Log Shrinks is %s',
                            'label'     : 'log_shrinks',
                            'query'     : BASE_QUERY % 'Log Shrinks',
                            'type'      : 'standard'
                            },
    
    'logtruncs'         : { 'help'      : 'Log Truncations',
                            'stdout'    : 'Log Truncations is %s',
                            'label'     : 'log_truncations',
                            'query'     : BASE_QUERY % 'Log Truncations',
                            'type'      : 'standard'
                            },
    
    'logwait'           : { 'help'      : 'Log Flush Wait Time',
                            'stdout'    : 'Log Flush Wait Time is %sms',
                            'label'     : 'log_wait_time',
                            'unit'      : 'ms',
                            'query'     : BASE_QUERY % 'Log Flush Wait Time',
                            'type'      : 'standard'
                            },
    
    'datasize'          : { 'help'      : 'Database Size',
                            'stdout'    : 'Database size is %sKB',
                            'label'     : 'KB',
                            'query'     : BASE_QUERY % 'Data File(s) Size (KB)',
                            'type'      : 'standard'
                            },
    
    'time2connect'      : { 'help'      : 'Time to connect to the database.' },
    
    'test'              : { 'help'      : 'Run tests of all queries against the database.' },
}


class DatabaseCheckOptions(CheckOptions):

    def extend_required_group(self,group):
        CheckOptions.extend_required_group(self,group)
        group.add_option('-T', '--table', help='Specify the table to check', default=None)

    def parse_args(self):
        options=CheckOptions.parse_args(self)
        if not options.table:
            self.error('Table is a required option.')
        return options

                
def parse_args():
    global MODES
    parser=DatabaseCheckOptions(MODES, usage = "usage: %prog -H hostname -U user -P password -T table --mode")
    return parser.parse_args()
    

   
if __name__ == '__main__':
    main(MODES, parse_args())
    
