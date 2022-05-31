from pathlib import Path
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join
from datetime import datetime
import fnmatch
import pandas as pd
from pandas import json_normalize
import numpy as np
from datetime import datetime
import httpagentparser
import json
from subprocess import PIPE, Popen
from os import listdir
import os
import argparse
import httpagentparser
parser = argparse.ArgumentParser()    
#add positional argument
parser.add_argument("word",help = "directory_path")
#add optional argument
parser.add_argument("-u",action="store_true",default = False,dest="timeFormat")
#parse the arguments
args = parser.parse_args()

basePath = Path(args.word)
# list for all files in a directory
files = [file for file in basePath.iterdir() if(file.is_file() & fnmatch.fnmatch(file,"*.json"))]
checksums = {}
# empty list for the duplicated checksums
duplicates = []
time = datetime.now()
def osname(agent):
   try:
       os=httpagentparser.detect(agent)['os']['name']
       return os   
   except:
       os=np.nan
   return os

# Iterate over the list of files
def browserName(agent):
   try:
       br=httpagentparser.detect(agent)['browser']['name']
       return br   
   except:
       br=np.nan
   return br
   
   
   
for filename in files:
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
        if checksum in checksums:
            duplicates.append(filename)
        
        else:    
            checksums[checksum] = filename
print(f"Found Duplicates: {duplicates}")

# Iterate over the list of files

for filename in duplicates:
        os.remove(filename)
print("is removed successfully")

for file in files:
        if(file not in duplicates):
            records = [json.loads(line) for line in open(file,'r')]
            df = pd.json_normalize(records)
            df=df[['a','r','u','cy','ll','tz','t','hc']]
            df['web_browser'] = df['a'].str.split(' ',expand = True,n = 1)[0]
 
            os = df['a'].str.split("(" , expand = True , n = 1)
            os = os[1].str.split(" ", expand = True , n = 1)[0]
            df['os']=os.str.replace(';',"")
    
            df['longitude'] = df['ll'].str[0]
            df['latitude'] = df['ll'].str[1]
            df.rename(columns={'cy':'city','r':'from_url','u':'to_url','tz':'timezone','t':'time_in','hc':'time_out'},inplace=True)
            df['from_url'] = df['from_url'].replace(r'http://', '',regex=True)
            df['from_url'] = df['from_url'].str.split('/', n=1, expand=True)[0]
            df['to_url']=df['to_url'].str.split('//', n=1, expand=True)[1]
            df['to_url']=df['to_url'].str.split('/', n=1, expand=True)[0]
            #drop nan 
            df = df.dropna()
	        #time and -u argument
            if(args.timeFormat):
                df['time_in'] = df['time_in']
                df['time_out'] =df['time_out']
            else:
                df['time_in'] = pd.to_datetime(df['time_in'])
                df['time_out'] =pd.to_datetime(df['time_out'])
            df=df[['web_browser','os','from_url','to_url','city','longitude','latitude','timezone','time_in','time_out']]
    

            print('There is  number of rows where  transformed from file is',len(df))
            print('there is directory path is',args.word)
	
	        #to convert it in csv files
            file = str(file).replace('.json',' ')
            df.to_csv(file+'.csv')
            
#for time 			
total_time = (datetime.now() - time)
print('Total Execuation Time {}'.format(total_time))

			
			