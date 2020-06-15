#imports
import pandas as pd
import datetime
import os
import influxdb

#automatic data gathering
start = datetime.datetime.now() - datetime.timedelta(30)
start = start.strftime("%Y-%m-%dT%H:%M:%S")

os.system('sacct -X -a -p -S '+start+' -o JobID,user,account,reserved,eligible,start,Partition,QOS,state > userdata.csv')
data = pd.read_csv("userdata.csv", sep = "|", header = 0)
df = pd.DataFrame(data)

#data cleaning/formatting
cdf = df[df.State.str.contains("COMPLETED")]
cdf = cdf[cdf.Partition.str.contains("compute")]
cdf = cdf[cdf.QOS.str.contains("batch")]

#deriving queue
cdf['Start'] = pd.to_datetime(cdf.Start)
cdf['Eligible'] = pd.to_datetime(cdf.Eligible)
cdf['Queue'] = cdf.Start - cdf.Eligible
cdf['Queue_Sec'] = (cdf.Queue.dt.days * 86400) + cdf.Queue.dt.seconds
cdf['Queue_Min'] = cdf['Queue_Sec']/60

#average queue/user
avg_queue_user_m = cdf.groupby('User')['Queue_Min'].mean()
avg_queue_lab_m = cdf.groupby('Account')['Queue_Min'].mean()

#populating HPCLive database
qulist = list()

for row in avg_queue_user_m.interrows():
    qu = {
    "measurement": "avg_queue_user",
    "tags": {"cluster": "sumner", "user": avg_queue_user_m['User']}
    "fields": { "avg_queue": avg_queue_user_m['Queue_Min']}
        
qulist.append(qu)

qllist = list()   
        
for row in avg_queue_lab_m.interrows():
    ql = {
    "measurement": "avg_queue_lab",
    "tags": {"cluster": "sumner", "lab": avg_queue_user_m['Account']}
    "fields": { "avg_queue": avg_queue_user_m['Queue_Min']}        }

qllist.append(ql)
