#!/usr/bin/env python
################### check_mssql_database.py ############################
# Version 2.0.2
# Date : Apr 4 2013
# Author  : Nicholas Scott ( scot0357 at gmail.com )
# Help : scot0357 at gmail.com
# Licence : GPL - http://www.fsf.org/licenses/gpl.txt
#
# Changelog : 
# 1.0.2 -   Fixed Uptime Counter to be based off of database
#           Fixed divide by zero error in transpsec
# 1.1.0 -   Fixed port bug allowing for non default ports | Thanks CBTSDon
#           Added batchreq, sqlcompilations, fullscans, pagelife | Thanks mike from austria
#           Added mode error checking which caused non-graceful exit | Thanks mike from austria
# 1.2.0 -   Added ability to specify instances
# 2.0.0 -   Complete rewrite of the structure, re-evaluated some queries
#           to hopefully make them more portable | Thanks CFriese
#           Updated the way averages are taken, no longer needs tempdb access
# 2.0.1 -   Fixed try/finally statement to accomodate Python 2.4 for
#           legacy systems
# 2.0.2 -   Fixed issues where the SQL cache hit queries were yielding improper results
#           when done on large systems | Thanks CTrahan
# 2.0.3 -   Remove misleading description of lockwait, removing the word Average -SW
# Modified 01/22/2015 Removed extraneous ';' from output. -BD-G
########################################################################

from check_lib import main, CheckOptions

BASE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s' AND instance_name='';"
INST_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s' AND instance_name='%s';"
OBJE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='%s';"
DIVI_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name LIKE '%s%%' AND instance_name='%s';"
CON_QUERY="SELECT count(*) FROM sys.sysprocesses;"
MEM_QUERY="SELECT 100*(1.0-(available_physical_memory_kb/(total_physical_memory_kb*1.0))) FROM sys.dm_os_sys_memory;" 
CPU_QUERY="SELECT "+\
	"record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [CPU] "+\
	"FROM ( "+ \
		"SELECT[timestamp], CONVERT(XML, record) AS [record] "+\
       		"FROM sys.dm_os_ring_buffers WITH ( NOLOCK ) "+\
       		"WHERE ring_buffer_type=N'RING_BUFFER_SCHEDULER_MONITOR' "+\
			"AND record LIKE N'%<SystemHealth>%'"+\
	") as x;"

MODES     = {
    'connections'	: { 'help'	: 'Number of open connections',
			    'stdout'	: 'Number of open connections is %s',
			    'label'	: 'connections',
			    'type'	: 'standard',
			    'query'	: CON_QUERY 
			  },	
    'memory'		: { 'help'	: 'Used server memory',
			    'stdout'    : 'Server uses %s%% of memory',
			    'label'	: 'memory',
			    'query'	: MEM_QUERY
			  },	
    'cpu'		: { 'help'	: 'Server CPU utilization',
			    'stdout'    : 'Current CPU utilization is %s%%',
			    'lablel'	: 'cpu',
			    'query'     : CPU_QUERY
			  },
    'bufferhitratio'    : { 'help'      : 'Buffer Cache Hit Ratio',
                            'stdout'    : 'Buffer Cache Hit Ratio is %s%%',
                            'label'     : 'buffer_cache_hit_ratio',
                            'unit'      : '%',
                            'query'     : DIVI_QUERY % ('Buffer cache hit ratio', ''),
                            'type'      : 'divide',
                            'modifier'  : 100,
                            },
    
    'pagelooks'         : { 'help'      : 'Page Lookups Per Second',
                            'stdout'    : 'Page Lookups Per Second is %s',
                            'label'     : 'page_lookups',
                            'query'     : BASE_QUERY % 'Page lookups/sec',
                            'type'      : 'delta'
                            },
    
    'freepages'         : { 'help'      : 'Free Pages (Cumulative)',
                            'stdout'    : 'Free pages is %s',
                            'label'     : 'free_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Free pages'
                            },
                            
    'totalpages'        : { 'help'      : 'Total Pages (Cumulative)',
                            'stdout'    : 'Total pages is %s',
                            'label'     : 'totalpages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Total pages',
                            },
                            
    'targetpages'       : { 'help'      : 'Target Pages',
                            'stdout'    : 'Target pages are %s',
                            'label'     : 'target_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Target pages',
                            },
                            
    'databasepages'     : { 'help'      : 'Database Pages',
                            'stdout'    : 'Database pages are %s',
                            'label'     : 'database_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Database pages',
                            },
    
    'stolenpages'       : { 'help'      : 'Stolen Pages',
                            'stdout'    : 'Stolen pages are %s',
                            'label'     : 'stolen_pages',
                            'type'      : 'standard',
                            'query'     : BASE_QUERY % 'Stolen pages',
                            },
    
    'lazywrites'        : { 'help'      : 'Lazy Writes / Sec',
                            'stdout'    : 'Lazy Writes / Sec is %s/sec',
                            'label'     : 'lazy_writes',
                            'query'     : BASE_QUERY % 'Lazy writes/sec',
                            'type'      : 'delta'
                            },
    
    'readahead'         : { 'help'      : 'Readahead Pages / Sec',
                            'stdout'    : 'Readahead Pages / Sec is %s/sec',
                            'label'     : 'readaheads',
                            'query'     : BASE_QUERY % 'Readahead pages/sec',
                            'type'      : 'delta',
                            },
                            
    
    'pagereads'         : { 'help'      : 'Page Reads / Sec',
                            'stdout'    : 'Page Reads / Sec is %s/sec',
                            'label'     : 'page_reads',
                            'query'     : BASE_QUERY % 'Page reads/sec',
                            'type'      : 'delta'
                            },
    
    'checkpoints'       : { 'help'      : 'Checkpoint Pages / Sec',
                            'stdout'    : 'Checkpoint Pages / Sec is %s/sec',
                            'label'     : 'checkpoint_pages',
                            'query'     : BASE_QUERY % 'Checkpoint pages/Sec',
                            'type'      : 'delta'
                            },
                            
    
    'pagewrites'        : { 'help'      : 'Page Writes / Sec',
                            'stdout'    : 'Page Writes / Sec is %s/sec',
                            'label'     : 'page_writes',
                            'query'     : BASE_QUERY % 'Page writes/sec',
                            'type'      : 'delta',
                            },
    
    'lockrequests'      : { 'help'      : 'Lock Requests / Sec',
                            'stdout'    : 'Lock Requests / Sec is %s/sec',
                            'label'     : 'lock_requests',
                            'query'     : INST_QUERY % ('Lock requests/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'locktimeouts'      : { 'help'      : 'Lock Timeouts / Sec',
                            'stdout'    : 'Lock Timeouts / Sec is %s/sec',
                            'label'     : 'lock_timeouts',
                            'query'     : INST_QUERY % ('Lock timeouts/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'deadlocks'         : { 'help'      : 'Deadlocks / Sec',
                            'stdout'    : 'Deadlocks / Sec is %s/sec',
                            'label'     : 'deadlocks',
                            'query'     : INST_QUERY % ('Number of Deadlocks/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'lockwaits'         : { 'help'      : 'Lockwaits / Sec',
                            'stdout'    : 'Lockwaits / Sec is %s/sec',
                            'label'     : 'lockwaits',
                            'query'     : INST_QUERY % ('Lock Waits/sec', '_Total'),
                            'type'      : 'delta',
                            },
    
    'lockwait'          : { 'help'      : 'Lock Wait Time (ms)',
                            'stdout'    : 'Lock Wait Time (ms) is %sms',
                            'label'     : 'lockwait',
                            'unit'      : 'ms',
                            'query'     : INST_QUERY % ('Lock Wait Time (ms)', '_Total'),
                            'type'      : 'standard',
                            },
    
    'averagewait'       : { 'help'      : 'Average Wait Time (ms)',
                            'stdout'    : 'Average Wait Time (ms) is %sms',
                            'label'     : 'averagewait',
                            'unit'      : 'ms',
                            'query'     : DIVI_QUERY % ('Average Wait Time', '_Total'),
                            'type'      : 'divide',
                            },
    
    'pagesplits'        : { 'help'      : 'Page Splits / Sec',
                            'stdout'    : 'Page Splits / Sec is %s/sec',
                            'label'     : 'page_splits',
                            'query'     : OBJE_QUERY % 'Page Splits/sec',
                            'type'      : 'delta',
                            },
    
    'cachehit'          : { 'help'      : 'Cache Hit Ratio',
                            'stdout'    : 'Cache Hit Ratio is %s%%',
                            'label'     : 'cache_hit_ratio',
                            'query'     : DIVI_QUERY % ('Cache Hit Ratio', '_Total'),
                            'type'      : 'divide',
                            'unit'      : '%',
                            'modifier'  : 100,
                            },
    
    'batchreq'          : { 'help'      : 'Batch Requests / Sec',
                            'stdout'    : 'Batch Requests / Sec is %s/sec',
                            'label'     : 'batch_requests',
                            'query'     : OBJE_QUERY % 'Batch Requests/sec',
                            'type'      : 'delta',
                            },
    
    'sqlcompilations'   : { 'help'      : 'SQL Compilations / Sec',
                            'stdout'    : 'SQL Compilations / Sec is %s/sec',
                            'label'     : 'sql_compilations',
                            'query'     : OBJE_QUERY % 'SQL Compilations/sec',
                            'type'      : 'delta',
                            },
    
    'fullscans'         : { 'help'      : 'Full Scans / Sec',
                            'stdout'    : 'Full Scans / Sec is %s/sec',
                            'label'     : 'full_scans',
                            'query'     : OBJE_QUERY % 'Full Scans/sec',
                            'type'      : 'delta',
                            },
    
    'pagelife'          : { 'help'      : 'Page Life Expectancy',
                            'stdout'    : 'Page Life Expectancy is %s/sec',
                            'label'     : 'page_life_expectancy',
                            'query'     : OBJE_QUERY % 'Page life expectancy',
                            'type'      : 'standard'
                            },
    
    #~ 'debug'             : { 'help'      : 'Used as a debugging tool.',
                            #~ 'stdout'    : 'Debugging: ',
                            #~ 'label'     : 'debug',
                            #~ 'query'     : DIVI_QUERY % ('Average Wait Time', '_Total'),
                            #~ 'type'      : 'divide' 
                            #~ },
    
    'time2connect'      : { 'help'      : 'Time to connect to the database.' },
    
    'test'              : { 'help'      : 'Run tests of all queries against the database.' },

}



def parse_args():
    global MODES
    p=CheckOptions(MODES, usage = "usage: %prog -H hostname -U user -P password --mode")
    return p.parse_args()

if __name__ == '__main__':
    main(MODES, parse_args())
