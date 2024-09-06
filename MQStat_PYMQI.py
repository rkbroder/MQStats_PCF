"""
Program to produce CSV file of MQ Statistics
Statistics must be turned on in the Queue Manager for them to be sent to SYSTEM.ADMIN.STSTISTICS Queue
Statistics can be turn on at the Queue Manager but it is advisable to turn them on at the Queue Level
    This will reduce the output and get specific on the queues tracked.
A good setup would be to trigger the scripts on the SYSTEM.ADMIN.STATISTICS.QUEUE to run as FIRST when messages show up.
The properties file that runs this script is typically in /var/mqm/scripts and looks like this:

[qmgrName]
key1=BOBBEE2
[MQConnection]
queuemanager=BOBBEE2
channel=SYSTEM.ADMIN.SVRCONN
ip=127.0.0.1
port=1414
ssl=NO
repos=/var/mqm/mqm
cipher=TLS_RSA_WITH_AES_256_CBC_SHA256
[reportcycle]
# CSV output file rotating values = monthly, daily, weekly
occurance=monthly


This program relies on the output of the mqtools program get_pcf.py. This can be installed from GitHub with:

pip3 -v install git+http://github.com/colinpaicemq/MQTools/

It uses the output from get_pcf.py like the pretty_json.py sample

The program runs with something like the following command:

python3.12 get_pcf.py -qm BOBBEE2 -channel SYSTEM.ADMIN.SVRCONN -conname '9.30.43.121(1414)' -userid mqm -password mqm -queue SYSTEM.ADMIN.STATISTICS.QUEUE | python3.12 MQStat_PYMQI.py

Requirements:
This program requires PYTHON 3.12.5 because of the CASE statement. This can be worked around for a lower level using IF statements
The program may require in PIP installation of modules. They are notated with a comment in the IMPORTS.

Input parameters:
none, does require the properties file

Output:
CSV Report of Queue Stats - There is a sample EXCEL sheet in the repository with a chart on the second tab. Inputting new data to the 
   first sheet updates the chart.

Functions:
  connect_queue_manager(queueManager)
  reset_stats(q_name, keys_csv, values_csv)
  mq_queue_manager_names()
  get_config_dict(section)
  recursive_items(dictionary)
  findkeys(node, kv)
  recurse_object(obj_to_inspect, val_to_find, indexpath='')
  find(element, JSON)
  create_output_file(headings, values)
  
"""

#_ctypes  yum install libffi-devel
#         yum install python3.12-devel

import calendar
import dateutil.parser    #pip3.12 install python-dateutil
from datetime import date
from datetime import datetime
import calendar
import configparser
import subprocess
import os.path
from os.path import exists as file_exists
import io
import codecs
import json
import pymqi  #pip3 install pymqi
import mqtools.mqpcf as MQPCF
import logging
import logging.config
import argparse
import sys
import getpass
import struct
import string
import mqtools.MQ as MQ # for formatMQMD
from collections import OrderedDict # the data appears to contain an OrderedDict. 
from nested_lookup import nested_lookup  #pip3.12 install nested-lookup

global qmgr, queueManager, indexpath, keys_csv, values_csv, countn

##
###
####
##      Predefined Variables
####
###
##
countn = 0
queueManager = ''
keys_csv = ''
values_csv = ''
proppath = '/home/mqm/scripts/' # path to the properties file
propname = 'MQStat_PYMQI.properties' # name of the properties file

##
###
####
##                 connect_queue_manager(queueManager)
## connect to the Queue Manager passed in client mode. Python default
####
###
##
def connect_queue_manager(queueManager):
##
### Check for connecion stanza (MQConnection). Then check for SSL to see if we  
### are connecting via SSL
##
 
  mq_connection_property = get_config_dict('MQConnection')
  property_found=True
  mq_connection_property = get_config_dict('MQConnection')
  ssl=mq_connection_property.get("ssl")
  host=mq_connection_property.get("ip")
  port=mq_connection_property.get("port")
  channel=mq_connection_property.get("channel")
  ssl_asbytes=str.encode(ssl)
  host_asbytes=str.encode(host)
  port_asbytes=str.encode(port)
  channel_asbytes=str.encode(channel)
  
###
### Check for ssl property to see if we are going to connect usig SSL
###
  if ssl == 'NO':
    conn_info = '%s(%s)' % (host, port)
    try:
      ##print("no SSL lets connect. queueManager = ", queueManager, " Channel = ", channel, "conn_info = ", conn_info)
      qmgr = pymqi.connect(queueManager, channel, conn_info)
      rc=True
    except pymqi.MQMIError as e:
      if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
        rc=False
      else:
        rc=False
        raise
  else:
    ##print("We have SSL.")
    conn_info = '%s(%s)' % (host, port)
    conn_info_asbytes=str.encode(conn_info)
    ssl_cipher_spec = mq_connection_property.get("cipher")
    ssl_cipher_spec_asbytes=str.encode(ssl_cipher_spec)
    repos = mq_connection_property.get("repos")
    repos_asbytes=str.encode(repos)
    cd = pymqi.CD()
    cd.ChannelName = channel_asbytes
    cd.ConnectionName = conn_info_asbytes
    cd.ChannelType = pymqi.CMQXC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQXC.MQXPT_TCP
    cd.SSLCipherSpec = ssl_cipher_spec_asbytes
    options = CMQC.MQCNO_NONE
    cd.UserIdentifier = str.encode('mqm')
    cd.Password = str.encode('mqm')
    sco = pymqi.SCO()
    sco.KeyRepository = repos_asbytes
#  qmgr.connect_with_options(queueManager, options, cd, sco)
    try:
       qmgr = pymqi.QueueManager(None)
       qmgr = qmgr.connect_with_options(queueManager, cd, sco)
       rc=True
    except pymqi.MQMIError as e:
       if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
       	 rc=False
       else:
         rc=False
         raise
  if rc:
    ##print("Returning true from CONNECT.")
    return qmgr
  else:
    return False

##
###
####
##                      reset_stats(q_name)
## This function extract the ENQ/DEQ rates for a queue from reset stats
####
###
##
def reset_stats(q_name, keys_csv, values_csv):
  qmgr = connect_queue_manager(queueManager)
  pcf = pymqi.PCFExecute(qmgr)
##
### Issure RESET STATS on Queue
##

  queue_args = {pymqi.CMQC.MQCA_Q_NAME: q_name}
                 
  try:
      queue_response = pcf.MQCMD_RESET_Q_STATS(queue_args)
      ##print("Queue_response = ", queue_response)
  except pymqi.MQMIError as e:
      if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
         rc=False
         return rc
      else:
         rc = False
         raise
  else:
      for queue_info in queue_response:
##
### get MQIA_MSG_DEQ_COUNT
##
        deq = str(queue_info[pymqi.CMQC.MQIA_MSG_DEQ_COUNT])
        ##print("DEQ rate = ", deq)
        values_csv = values_csv + deq + "," 
        keys_csv = keys_csv + "DEQ Rate"+ "," 
##
### get MQIA_MSG_ENQ_COUNT
##
        enq = str(queue_info[pymqi.CMQC.MQIA_MSG_ENQ_COUNT])
        ##print("DEQ rate = ", enq)  
        values_csv = values_csv + enq + "," 
        keys_csv = keys_csv + "ENQ Rate" + ","
      ##print("-----keys_csv in RESET_STATS = ", keys_csv)
      ##print("-----values_csv in RESET_STATS = ", values_csv)
      rc = True 
  qmgr.disconnect()     
  return keys_csv, values_csv   
 
##
###
####
##                mq_queue_manager_names()
##  Get a list of Queue Managers
####
###
##
def mq_queue_manager_names():
  # lets get the MQ Version from the property file
  config_details = get_config_dict('qmgrName')
  qmgrkey = config_details['key1']
  ##print("-----Queue manager Name = ", qmgrkey)
  managerName = qmgrkey.strip()
  ##print("-----Queue Manager Name from get Names = ", managerName)
  if managerName is not None:
    return managerName
  else:
    return ''
  
##
###
####
##                   get_config_dict(section)
## This function will process load the sections from the property
## file into the script.
####
###
##
def get_config_dict(section):
    get_config_dict.config_dict = dict(config.items(section))
    return get_config_dict.config_dict

##
###
####
##                   recursive_items(dictionary)
## This function will process a dictionary for key, value
####
###
##
def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            yield (key, value)
            yield from recursive_items(value)
        else:
            yield (key, value)
            
##
###
####
##                         findkeys(node, kv)
## This routine will parse through a given DICTIONARY with nested dictionaries.
## It will display the keys and values of ALL keys.
####
###
##
def findkeys(node, kv):
    if isinstance(node, list):
        for i in node:
            for x in findkeys(i, kv):
               yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in findkeys(j, kv):
                yield x
                

##
###
####
##           recurse_object(obj_to_inspect, val_to_find, indexpath='')
## This routine will parse through a given DICTIONARY and search for a given key.
## It will return the PATH to the KEY. this is used to PATH to a KEY in a nested
## dictionary.
####
###
##
def recurse_object(obj_to_inspect, val_to_find, indexpath=''):
    global patho 
    if isinstance(obj_to_inspect, dict):
        for key, value in obj_to_inspect.items():
            recurse_object(value, val_to_find, indexpath + f"['{key}']")
    if isinstance(obj_to_inspect, list):
        for key, value in enumerate(obj_to_inspect):
            recurse_object(value, val_to_find, indexpath + f"[{key}]")
    if isinstance(obj_to_inspect, str):
        if obj_to_inspect == val_to_find:
#            print(f"Value {val_to_find} found at {indexpath}")
            patho=indexpath
            return patho
            
##
###
####
##                       find(element, JSON)
## 
####
###
## 
def find(element, JSON):     
  paths = element.split(".")  
  # print JSON[paths[0]][paths[1]][paths[2]][paths[3]]
  for i in range(0,len(paths)):
    data = JSON[paths[i]]
    # data = data[paths[i+1]]
#    print(data)

##
###
####
##                         create output file()
## This function creates or reuses the ouput CSV file. It is also driven
## by the properties file for the file occurance (daily, weekly, monthly)
## If the file name exists we will append. Otherwise create a new one
####
###
##    
def create_output_file(headings, values):
  hostname = subprocess.check_output("hostname", shell=True, universal_newlines=True)
  list0=hostname.split(".")
  hostHLQ=list0[0].strip()	
  reportcycle = get_config_dict('reportcycle')
  occurance=reportcycle.get("occurance")
  print("Report Cycle = ", occurance)
###
### Set up report file name and figure out if we create or reuse
###
  match occurance:
  
    case "monthly":
      occurance_value = calendar.month_name[date.today().month]
      name = "/home/mqm/scripts/" + hostHLQ + ".QUEUE_STATS_" + occurance_value
      filename = "%s.csv" % name
      
      if file_exists(filename):
        report = open(filename, "at")
      else:
        report = open(filename, "wt")
        outputL=headings + "\n"
        report.write(outputL)	
        
    case "daily":
      occurance_value= datetime.today().strftime('%Y-%m-%d')
      name = "/home/mqm/scripts/" + hostHLQ + ".QUEUE_STATS_" + occurance_value
      filename = "%s.csv" % name
      if file_exists(filename):
        report = open(filename, "at")
      else:
        report = open(filename, "wt")
        outputL=headings + "\n"
        report.write(outputL)	
        
    case "weekly":
      name = "/home/mqm/scripts/" + hostHLQ + ".QUEUE_STATS_" + occurance
      filename = "%s.csv" % name # this is the name of the file
      todaydt = datetime.now() # lets get the day of the week to see if it is Sunday
      today = todaydt.isoweekday()
      d2 = datetime.today().strftime('%Y-%m-%d')
      
      if not file_exists(filename): # file does not exist at all
        report = open(filename, "wt")
        outputL=headings + "\n"
        report.write(outputL)
        
      else: # file does exist
#        filecreation = os.path.getctime(filename)
#        create_date = date.fromtimestamp(filecreation).strftime('%m-%d-%y')
#        create_time = os.path.getctime(filename)
#        print(create_time)
#        create_date = datetime.strptime(create_time, "%Y-%m-%d")
#        print('Created on:', create_date)
#        numdays = abs((d2 - create_date).days)
        file_time = os.path.getmtime(filename)
        if (today == 6) and (((time.time() - file_time) / 3600 > 24*days) > 1): # it's Sunday, file is there and it is older than today
          ## print("-----Rename weekly file and create new.")
          os.rename(filename, filename + d2) #rename old file with date # rename old file, add todays date to the name
          report = open(filename, "wt")
          outputL=headings + "\n"
          report.write(outputL)
          
        else: # we will reuse the existing file
          ## print("-----Reuse weekly file")
          report = open(filename, "at")
  outputL=values + "\n"
  report.write(outputL)
  return report
    
##
###
####
## Python3 code to Read the serailzed dictionary created by get_pcf.py routine of the mqtools installment.
## the code will load the dictionary of MQPCF Statistics Events. It will isolate non-SYSTEM queues and
## write the results in CSV format to a file after adding DEQ and ENQ Reset Stats metrics to the list
## of values. this report is a daily report that is appended to everytime the routine is run.
##
## It is expected MQ Stats are set up on a Queue Level for targeted queues at a refesh rate selected by the 
## user. The script is used on the SYSTEM.ADMIN.STATISTICS.QUEUE using Triggering to process messages as 
## they appear.
##
## The CSV file can be imported into the provided EXCEL sheet. the Queue can be selected from the Q_NAME
## drop down which will refresh the chart on the second worksheet to show Queue Depth and the DEQ/ENQ
## rates.
####
###
##                                                                Bobbee Broderick - rkbroder@us.ibm.com

##
###
#### Get the properties file
###
##
prop_path = proppath + propname
config = configparser.RawConfigParser()

if os.path.exists(prop_path):
  config.read(prop_path)
else:
  print("-----No property File")
  raise
    
queueManager =  mq_queue_manager_names()
if queueManager is None:
  print("-----Queue Manager Name FAULT")
  raise
#else:
#	print("Do we have a Queue Manager Name = ", queueManager)
while True:
  # Input stream
  mqpcf_stats = sys.stdin.readline()
  if not mqpcf_stats:
    break
 
#print("----Type mqpcf_stats",type(mqpcf_stats))
  new_test_dict = json.loads(mqpcf_stats)
  ##print("----Type new_test_dict",type(mqpcf_stats))
    
###
### open the serailzed dictionary file from get_pcf.py
###
    
  countn += 1    
#print("----Type new_test_dict",type(new_test_dict))
  ##print("called "+str(countn), file=sys.stderr, flush=True)
#print("-----The new dictionary is : " + str(new_test_dict))

  keys_list = list(new_test_dict.keys())
  
# Printing the keys
#print("Method 1 - Using keys() method:")
#print(keys_list)
#print("Method 2 - Using for loop method:")
#for key in new_test_dict:
#    print(key)
    
#print("Method 3 - recursive looping method:")
  for key, value in recursive_items(new_test_dict):
    if not isinstance(value, dict):
#        print("Out of Recursive routine KEY =", key, "VALUE = ", value)
#        print("Out of Recursive routine VALUE type", type(value))
      continue      
    
#print("Method 4 - find Q_NAME:")
  QNAME_LIST = list(findkeys(new_test_dict, 'Q_NAME'))
#print("----Type QNAME_LIST",type(QNAME_LIST))
#print("-----QNAME_LIST = ", list(QNAME_LIST))
#QNAME_LIST_LIST = list(QNAME_LIST))
#print(list(findkeys(new_test_dict, 'Q_NAME')))

##
## This will call recurse_object() to get the path to Q_NAME(s).
## This will allow us to get the keys and values for that queue
##
  reportcalled = False
#print("Method 4 - find path to Q_NAME:")
  for queue in QNAME_LIST:
    keys_csv = ''
    values_csv = ''
#  print("----Q_NAME_LIST = ", queue)
 
    recurse_object(new_test_dict, queue)
#  print("Patho type = ", type(patho))
#  print("----Path to data(patho) = ", patho)
  
    outstring = ''
    count=patho.count(']')
#  print("Count = ", count)
    x = patho.split("]", count-1)
    count = count - 1
  
    for qualifiers in x:
      if count > 0:
        outstring = outstring  + qualifiers + "]"
        count = count - 1
#  print("-----outstring = ", outstring)

    fullpathkeys = "new_test_dict" + str(outstring) + ".keys()" + "\n"
    fullpathvalues = "new_test_dict" + str(outstring) + ".values()" + "\n"
#  print("-----fullpathvalues value = ", fullpathkeys)
#  print("-----fullpathkeys value = ", eval(fullpathvalues))
#  print("-----Keys of SYSTEM.ADMIN.COMMAND.QUEUE = ", new_test_dict['PCFData']['Q_STATISTICS_DATA']['SYSTEM.ADMIN.COMMAND.QUEUE'].keys())
    keys_string = eval(fullpathkeys)
    value_string = eval(fullpathvalues)
#  print("-----keys_string type = ", type(keys_string))
#  print("-----value_string type = ", type(value_string))
    keys_string_print = ",".join(list(keys_string))
#  value_string_print = ",".join(list(value_string))
#  print("----- string of keys = ", keys_string_print)
#  print("----- string of value = ", value_string_print)
  
#  print("Keys of patho = ", new_test_dict.keys()) 
  
    for q_string in keys_string:
      keys_csv = keys_csv + q_string + ','
#	print("-----key of SYSTEM.ADMIN.COMMAND.QUEUE = ",q_string)
#	print("-----q_string 1 type = ", type(q_string))
  
    for q_value in value_string:
      typeit = str(type(q_value))
#  print("-----typeit type = ", type(typeit))
#  print("-----typeit value = ", str(typeit))
      if "int" in typeit.lower():
         val = str(q_value)
      elif "str" in typeit.lower():
         val = q_value
      elif "list" in typeit.lower():
         total = 0
         for listvalue in q_value:
           total = total + int(listvalue)
         val = str(total)	  
      else:
         raise ValueError("Unknown data type:", typeit.lower())
      values_csv = values_csv + str(val) + ','
    keys_csv, values_csv = reset_stats(queue, keys_csv, values_csv)
  ##print("-----keys_csv back from RESET_STATS = ", keys_csv)
  ##print("-----values_csv back from RESET_STATS = ", values_csv)
  ##print(dir(datetime))
    dt = datetime.now()
    ##print("-----values_csv before strip = ", values_csv)
    keys_csv = keys_csv.rstrip(",")
    values_csv = values_csv.rstrip(",")
    ##print("-----values_csv after strip = ", values_csv)
    keys_csv = keys_csv + "," + "Date-Time"
    values_csv = values_csv + "," + str(dt)
##
## We only want to call create_output_file once to open the file and get 
## the first line and/or headings out there
##
    ##print("------calling create_output_file")
    if reportcalled:
      outputL=values_csv + "\n"
      qstatsreport.write(outputL)
    else:
      ##print("------calling create_output_file")
      qstatsreport = create_output_file(keys_csv, values_csv)
      reportcalled = True
  
qstatsreport.close()
