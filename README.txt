Program to produce CSV file of MQ Statistics
Statistics must be turned on in the Queue Manager for them to be sent to SYSTEM.ADMIN.STSTISTICS.QUEUE.
Statistics can be turn on at the Queue Manager but it is advisable to turn them on at the Queue Level. 
This will reduce the output and get specific on the queues tracked.

A good setup would be to trigger the scripts on the SYSTEM.ADMIN.STATISTICS.QUEUE to run as FIRST when messages show up.
Examples of this setup is in the Repository

Below is the Properties file that runs this script is typically in /var/mqm/scripts and looks like this:

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

It uses the output from get_pcf.py like the pretty_json.py sample supplied in that repository. The MQStats_PCF program should be loaded into the
/examples directory under the mqtools installation

The program runs with something like the following command:

python3.12 get_pcf.py -qm BOBBEE2 -channel SYSTEM.ADMIN.SVRCONN -conname '9.30.43.121(1414)' -userid mqm -password mqm -queue SYSTEM.ADMIN.STATISTICS.QUEUE | python3.12 MQStat_PYMQI.py

Requirements:
This program requires PYTHON 3.12.5 because of the CASE statement. This can be worked around for a lower level using IF statements
The program may require PIP installation of modules used by the program. They are notated with a comment in the IMPORTS.

Input parameters:
1 - None. It does take a std input feed piped to the program from get_pcf.py
2 - Does require the properties file

Output:
CSV Report of Queue Stats - There is a sample EXCEL sheet in the repository with a chart on the second tab. Inputting new data to the 
   first sheet updates the chart. The chart has Message numbers on the Y axis and Time on the X axis
