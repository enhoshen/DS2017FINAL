#import panda as pd
import requests
import sys , os
import getpass
import pandas as pd
from functools import reduce
from timehelper import *
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

    def dataload(self,restore='False'):
        if restore == 'False':
            self.tp = pd.read_csv(self.filepaths['trip.csv']).drop  ( self.attrdrops['trip'],axis=1)
            self.st = pd.read_csv(self.filepaths['station.csv']).drop(self.attrdrops['station'],axis=1)
            self.wt = pd.read_csv(self.filepaths['weather.csv']).drop(self.attrdrops['weather'],axis=1)
        elif restore == 'True':
            #self.tp = pd.read_csv(self.filepaths['parsedtrip.csv'])
            #self.st = pd.read_csv(self.filepaths['parsedstation.csv'])
            self.wt = pd.read_csv(self.filepaths['parsedweather.csv'])
            
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
    def store(self):
        self.wt['wdate']=self.wt['wdate'].apply(time2str )
        self.tp['stime']=self.tp['stime'].apply(time2str )
        self.tp['sdate']=self.tp['sdate'].apply(time2str )
        self.tp['etime']=self.tp['etime'].apply(time2str )
        self.tp['edate']=self.tp['edate'].apply(time2str )
 
        self.wt.to_csv(os.path.join(self.dir,'parsedweather.csv'))        
class SFbay ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = os.path.join(self.datasetDir,'SFBAY')
        self.attrmapfile = os.path.join(self.datasetDir,'SF_attr_map.csv')
        self.filenames = ['weather.csv','trip.csv','status.csv','station.csv','parsedweather.csv','parsedtrip.csv','parsedstation.csv']
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
    def dataparse(self):
        self.loadnrename()
        self.timeparse()
         

if __name__ == '__main__':
    sf = SFbay()
    sf.dataparse()
    print(sf.wt['wdate'])
    sf.store()
    sf.dataload('True')
    print(sf.wt['wdate'])
    print(sf.wt['wdate'][0])
