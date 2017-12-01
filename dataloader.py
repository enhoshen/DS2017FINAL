#import panda as pd
import requests
import sys , os
import getpass
class dataloader () :
    def __init__(self):
        self.getkaggleinfo()
        
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
class SFbay ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = './SF'
        self.data_url = {}
        self.data_url['weather']= 'https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/weather.csv'
        self.data_url['trip']   =   'https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/trip.csv'
        self.data_url['status']=   'https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/status.csv'
        self.data_url['station']=  'https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/station.csv'    
        #self.data_url['test'] = 'http://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/weather.csv'
    def download (self):
        self.kaggledownload(self.dir)

class SeattleCycleShare ( dataloader ):
    def __init__(self):
        dataloader.__init__(self)
        self.dir = './SCS'
        self.data_url = {}
        self.data_url['station'] ='https://www.kaggle.com/pronto/cycle-share-dataset/downloads/station.csv'
        self.data_url['trip']    ='https://www.kaggle.com/pronto/cycle-share-dataset/downloads/trip.csv'
        self.data_url['weather'] ='https://www.kaggle.com/pronto/cycle-share-dataset/downloads/weather.csv' 

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
    sf.download()
