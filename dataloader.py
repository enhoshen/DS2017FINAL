#import panda as pd
import requests
import sys , os
import getpass
import pandas as pd
from functools import reduce
def str2time ( s , dtype='date'):
    s=s.split(' ')
    if dtype == 'date':
        d=  s[0].split('/')
        return [int(d[2]),int(d[0]),int(d[1])]
    elif dtype == 'time':
        t = s[1].split(':')
        return [int(t[0]),int(t[1])]
def timesel ( df ,column,  start , end):
    tempdf = df
    s = reduce( lambda x, y : 100*x+y , start)
    e = reduce( lambda x, y : 100*x+y , end)
    l = len(start)
    tempdf =tempdf[ tempdf[column].apply(lambda x : reduce(lambda a,b: 100*a+b, x[:l] )>= s) ]
    if len(tempdf)==0:
        return
    l = len(end)
    tempdf =tempdf[ tempdf[column].apply(lambda x : reduce(lambda a,b: 100*a+b, x[:l] )<= e) ]
    return tempdf
def timesel ( df , column, time ):
    return df [ df[column].apply( lambda x : reduce(lambda a,b: a and b , [x[i]==v for i,v in enumerate(time)]) ) ]
class dataloader () :
    def __init__(self):
        #self.getkaggleinfo()
        self.datasetDir = './data' 
    def getkaggleinfo(self):
        self.kaggle_info ={}
        self.kaggle_info['UserName']=input("enter your kaggle account ")
        print (self.kaggle_info['UserName'])
        self.kaggle_info['Password']=getpass.getpass("enter your password ")
    def kaggledownload(self , path): 
        for name,url in self.data_url.items():
            r = requests.get( url)
            r = requests.post(r.url , data=self.kaggle_info , stream = True )
            with open (os.path.join( path,name+'.csv') , 'wb')  as f:
                for chunk in r.iter_content(chunk_size = 512*1024):
                    if chunk:
                        f.write(chunk)      

    def dataload(self):
        self.tp = pd.read_csv(self.filepaths['trip.csv']).drop  ( self.attrdrops['trip'],axis=1)
        self.st = pd.read_csv(self.filepaths['station.csv']).drop(self.attrdrops['station'],axis=1)
        self.wt = pd.read_csv(self.filepaths['weather.csv']).drop(self.attrdrops['weather'],axis=1)
    def attrmapload(self):
        self.attrmap = {}
        with open ( self.attrmapfile) as f:
            cur_table = ''
            cur_map  = {}
            for line in f.readlines():
                line = line.split('\n')[0]
                line = line.split(',')
                if(line[1] == ''):
                    self.attrmap[cur_table]=cur_map
                    cur_table=line[0]
                    cur_map = {}
                else: 
                    cur_map[line[0]] = line[1] 
            self.attrmap[cur_table]=cur_map
        return self.attrmap
    def attrrename(self):
        self.st=self.st.rename(columns=self.attrmap['station'],)
        self.tp=self.tp.rename(columns=self.attrmap['trip'],)
        self.wt=self.wt.rename(columns=self.attrmap['weather'],)
    def loadnrename(self):
        self.dataload()
        self.attrmapload()
        self.attrrename()
    def timeparse(self):
     
        self.wt['wdate']=self.wt['wdate'].apply(str2time,args=('date',) )
        self.tp['stime']=self.tp['sdate'].apply(str2time,args=('time',) )
        self.tp['sdate']=self.tp['sdate'].apply(str2time,args=('date',) )
        self.tp['etime']=self.tp['edate'].apply(str2time,args=('time',) )
        self.tp['edate']=self.tp['edate'].apply(str2time,args=('date',) )
        '''
        self.wt['wdate']=pd.to_datetime(self.wt['wdate'])
        self.tp['sdate']=pd.to_datetime(self.tp['sdate'])
        self.tp['edate']=pd.to_datetime(self.tp['edate'])
        '''
class SFbay ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = os.path.join(self.datasetDir,'SFBAY')
        self.attrmapfile = os.path.join(self.datasetDir,'SF_attr_map.csv')
        self.filenames = ['weather.csv','trip.csv','status.csv','station.csv']
        self.filepaths = { x:os.path.join(self.dir, x ) for x in self.filenames}
        # unused attributes
        self.attrdrops = {'weather':['cloud_cover','wind_dir_degrees','zip_code'], 
                          'trip':['start_station_name','end_station_name','zip_code'],
                          'station':['name','city','installation_date',]  }
        self.base_url ='https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/' 
        self.data_url = [self.base_url+x for x in self.filenames]
    def download (self):
        self.kaggledownload(self.dir)
    def dataparse(self):
        self.loadnrename()
        self.timeparse()
        
        return 
class CYShare ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = os.path.join(self.datasetDir,'CYCLESHARE')
        self.attrmapfile = os.path.join(self.datasetDir,'CYS_attr_map.csv')
        self.filenames = [ 'station.csv','trip.csv','weather.csv']
        self.filepaths = { x:os.path.join(self.dir, x ) for x in self.filenames } 
        self.attrdrops = {'weather':[], 
                          'trip':['from_station_name','to_station_name'],
                          'station':['decommission_date','name','install_date','install_dockcount','modification_date']  }
        self.base_url ='https://www.kaggle.com/benhamner/pronto/cycle-share-dataset/downloads/' 
        self.data_url = [self.base_url+x for x in self.filenames]
    def download (self):
        self.kaggledownload(self.dir)

## The direct link to the Kaggle data set
#data_url = 'http://www.kaggle.com/c/digit-recognizer/download/train.csv'
#
## The local path where the data set is saved.
#local_filename = "train.csv"
#
## Kaggle Username and Password
#kaggle_info = {'UserName': "my_username", 'Password': "my_password"}
#
## Attempts to download the CSV file. Gets rejected because we are not logged in.
#r = requests.get(data_url)
#
## Login to Kaggle and retrieve the data.
#r = requests.post(r.url, data = kaggle_info, prefetch = False)
#
## Writes the data to a local file one chunk at a time.
#f = open(local_filename, 'w')
#for chunk in r.iter_content(chunk_size = 512 * 1024): # Reads 512KB at a time into memory
#    if chunk: # filter out keep-alive new chunks
#        f.write(chunk)
#f.close()         

if __name__ == '__main__':
    sf = SFbay()
    sf.dataparse()
    print(sf.wt['wdate'])
    t = timesel(sf.wt,'wdate',[2013,10])
    print(t['wdate'])
