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
        self.usedfiles = {'tp':'trip.csv','st':'station.csv','wt':'weather.csv'}
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

    def dataload(self,restore=False):
        if restore == False:
            self.dt = { d:pd.read_csv(self.filepaths[f]).drop(self.attrdrops[f],axis=1) for d,f in self.usedfiles.items()}
            self.attrmapload()
            self.attrrename()
            self.timeparse()
            self.eventparse()
            self.trim()
        else :
            self.dt = { d:pd.read_csv(self.parsedfilepaths['parsed'+f]) for d,f in self.usedfiles.items() }
            self.timeparse(restore=True)
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
        self.dt = { d: self.dt[d].rename(columns=self.attrmap[f.split('.csv')[0] ],) for d,f in self.usedfiles.items()}
    def timeparse(self,restore=False):
        table = {'wt':[('wdate','wdate')] , 
                 'tp':[ ('stime','sdate'),('sdate','sdate'),('etime','edate'),('edate','edate') ] }
        if restore == True:
            for x , i in table.items():
                for attr in i:
                    self.dt[x][attr[0]] = self.dt[x][attr[0]].apply(parsedstr2time) 
        else:
           for x,i in table.items():
                for attr in i:
                    self.dt[x][attr[0]]= self.dt[x][attr[1]].apply(str2time,args=( attr[0][1:], ))
    def store(self):
        for d,f in self.usedfiles.items():
            self.dt[d].to_csv(os.path.join(self.dir,'parsed'+f), index= False )
    def eventparse(self):
        return
    def trim(self):
        return
class SFbay ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = os.path.join(self.datasetDir,'SFBAY')
        self.attrmapfile = os.path.join(self.datasetDir,'SF_attr_map.csv')
        self.filenames = ['weather.csv','trip.csv','status.csv','station.csv']
        self.filepaths = { x:os.path.join(self.dir, x ) for x in self.filenames}
        self.parsedfilepaths = {'parsed'+x:os.path.join(self.dir,'parsed'+x) for x in self.filenames }
        # unused attributes
        self.attrdrops = {'weather.csv':['cloud_cover','wind_dir_degrees','zip_code'], 
                          'trip.csv':['start_station_name','end_station_name','zip_code'],
                          'station.csv':['name','city','installation_date',]  }
        self.base_url ='https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/' 
        self.data_url = [self.base_url+x for x in self.filenames]
    def download (self):
        self.kaggledownload(self.dir)
    
class CYShare ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = os.path.join(self.datasetDir,'CYCLESHARE')
        self.attrmapfile = os.path.join(self.datasetDir,'CYS_attr_map.csv')
        self.filenames = [ 'station.csv','trip.csv','weather.csv']
        self.filepaths = { x:os.path.join(self.dir, x ) for x in self.filenames }
        self.parsedfilepaths = {'parsed'+x:os.path.join(self.dir,'parsed'+x) for x in self.filenames }
        self.attrdrops = {'weather.csv':[], 
                          'trip.csv':['from_station_name','to_station_name'],
                          'station.csv':['decommission_date','name','install_date','install_dockcount','modification_date']  }
        self.base_url ='https://www.kaggle.com/benhamner/pronto/cycle-share-dataset/downloads/' 
        self.data_url = [self.base_url+x for x in self.filenames]
    def download (self):
        self.kaggledownload(self.dir)
    def eventparse(self):
        self.dt['wt']['events']=self.dt['wt']['events'].apply(lambda x : x.replace(' , ','-') if x==x  else x  )
    def trim (self):
        table = ['8D OPS 02','Pronto shop 2','Pronto shop']
        self.sel = self.dt['tp']['ssid']!=self.dt['tp']['ssid'] 
        for x in table:
            for y in ['ssid','esid']:
                self.sel = self.sel | (self.dt['tp'][y] == x)
        self.dt['tp'] = self.dt['tp'][~self.sel]

if __name__ == '__main__':
    cy = CYShare()
    cy.dataload('True')

    infos = cy.info4collector()
    collector = datacollector(infos)
    collector.save_comb_info()
    #print(collector.load_task1_data([2014,12], [2015,1]))
