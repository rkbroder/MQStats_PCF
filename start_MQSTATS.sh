#!/usr/bin/ksh
#####################################################################
#
#	This script will start the PYTHON MQ Stats Collection program 
#        It assumes a default location for PYTHON and the script
#       
#     Created: 9/6/2024
# Last Update: 
#	
#####################################################################
#
#
python3.12 /usr/local/lib/python3.12/site-packages/mqtools/examples/get_pcf.py -qm BOBBEE2 -channel SYSTEM.ADMIN.SVRCONN -conname '9.30.43.121(1414)' -userid mqm -password mqm -queue SYSTEM.ADMIN.STATISTICS.QUEUE | python3.12 /usr/local/lib/python3.12/site-packages/mqtools/examples/MQStat_PYMQI.py
