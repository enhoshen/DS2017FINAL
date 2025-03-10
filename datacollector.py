import os
import math
import time
import datetime
import dateutil.relativedelta as dr
import dataloader as dl
from dataloader import *
import numpy as np
import pandas as pd
from timehelper import *
import time
from math import sin, cos, sqrt, atan2, radians

class  datacollector():
    """docstring for  datacollector"""
    def __init__(self, dl):
        df_st = pd.DataFrame(dl.dt['st'])
        self.id2stTable = { x:df_st['sid'][x] for x in df_st.index }
        self.st2idTable = { df_st['sid'][x] :x for x in df_st.index}
        df_dist_st = self.get_station_distinfo(df_st)
        self.df_st = pd.concat([df_st, df_dist_st], axis=1) 

        self.df_tp = pd.DataFrame(dl.dt['tp'])
        self.df_wt = pd.DataFrame(dl.dt['wt'])
        self.wt_idx= { v[0]*10000+v[1]*100+v[2]: i for i,v in enumerate(self.df_wt['wdate']) }
        for col in ['ssid','esid']:
            self.df_tp[col] = self.df_tp[col].apply( lambda x : self.st2idTable[x] ) 

        o_dir = dl.dir
        self.table_path = os.path.join(o_dir, 'combin_info_table.csv')
        self.comb_info_path = os.path.join(o_dir, 'combin_info.csv')
        self.station_info_path = os.path.join(o_dir, 'station_info.csv')
        self.comb_trip_path = os.path.join(o_dir,'comb_trip.csv')
        self.st_hist_path = os.path.join(o_dir,'st_hist.json')
    def distance(self, p0, p1):
        R = 6373.0

        lat1 = radians(p0[0])
        lon1 = radians(p0[1])
        lat2 = radians(p1[0])
        lon2 = radians(p1[1])

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c
        return distance

    def get_station_distinfo(self, df_st):
        # 1.calculate the distance between stations
        # 2. record the min distance between stations
        points = list(zip(df_st['lat'], df_st['long']))
        st_num = len(points)
        points_dist = np.zeros((st_num, st_num))
        min_dist = np.ones((st_num,1))
        min_station = ["" for x in range(st_num)]
        
        for i in range(st_num):
            for j in range(st_num):
                dist = self.distance(points[i],points[j])
                points_dist[i,j] = dist
                if (i != j) and (dist < min_dist[i]):
                    min_dist[i] = dist
                    min_station[i] = j
        dist_list = [ x for x in points_dist ]
        df_dist_info = pd.DataFrame(index = range(len(df_st)))
        df_dist_info['mindist'] = min_dist
        df_dist_info['mindist_sid'] = min_station
        '''
        for i in range(st_num):
            newcolname = 'dist2_' + df_st['sid'][i]
            df_dist_info[newcolname] = points_dist[i,:]
        '''
        df_dist_info['dist'] = dist_list 
        # calculate the density of the station (staion numbers in average distance)
        avg_dist = np.mean(points_dist)
        density  = np.zeros((st_num,1))
        for i in range(st_num):
            density[i] = len(np.where(points_dist[0,:] < avg_dist)[0])-1
        
        df_dist_info['density'] = density

        return df_dist_info
    def get_st_wt_comb_info(self,restore=False,save=True):
        
        if restore and os.path.exists(self.comb_trip_path):
            csvfile = pd.read_csv(self.table_path, encoding='utf8')
            self.df_tp = pd.DataFrame(csvfile)
            table = ['sdate', 'stime', 'edate', 'etime']
            for attr in table:
                self.df_tp[attr] = self.df_tp[attr].apply(parsedstr2time)
        else :
            t= time.time()
            print ('start')
            self.df_tp  = self.df_tp.fillna(0)
            self.df_tp['tp_dist'] = self.df_tp.apply( lambda x :self.df_st['dist'][x['ssid'] ][x['esid'] ] , axis=1)
            print (time.time() -t)
            t= time.time()   
            df_tp_wt = self.df_tp['sdate'].apply(lambda x : self.df_wt.iloc[self.wt_idx[x[0]*10000+x[1]*100+x[2] ] ] ) 
            print (time.time() -t)
            t= time.time()   
 
            self.df_tp = pd.concat( [self.df_tp,df_tp_wt ] ,axis=1) 
            self.df_tp.to_csv(self.comb_trip_path,index=False)
        df_stp = self.df_tp.copy()
        df_etp = self.df_tp.copy()
        
        
        print (time.time() -t)
        t= time.time()


        df_stp['is_sst'] = df_stp['ssid'] == df_stp['ssid']  
        df_etp['is_sst'] = ~df_stp['is_sst']
       
        st_col = ['dcnt' , 'mindist', 'mindist_sid','density'] 
        tmp_st = self.df_st[st_col]
        st = df_stp['ssid'].apply(lambda x : tmp_st.iloc[int(x)] )
        df_stp = pd.concat( [df_stp,st] , axis=1)
        print (time.time() -t)
        t= time.time()

        st = df_etp['esid'].apply(lambda x : tmp_st.iloc[int(x)]  )
        df_etp = pd.concat( [df_etp,st] , axis=1)
        print (time.time() -t)
        t= time.time()

   
        self.df_tp = pd.concat([df_stp,df_etp], axis=0 )
        if save :
            self.df_tp.to_csv(self.comb_info_path,sep=',',encoding='utf-8',index=False)
        print ('check')   
    def savecomb_info(self):
        self.df_tp.to_csv(self.comb_info_path,sep=',',encoding='utf-8',index=False)
 
    def st_usage_freq(self, sid, stime, etime):
        df_intime = timesel_interval(self.df_tp, 'sdate' ,stime, etime)

        sday = datetime.date(*stime)
        eday = datetime.date(*etime)
        diff = eday - sday
        days = float(diff.days)
        sn = float(len(df_intime[df_intime['ssid'] == sid].index)/2)
        en = float(len(df_intime[df_intime['esid'] == sid].index)/2)
        freq = (sn+en) / days
        return freq
    def parsebirth (self):
        self.df_tp['birth']=self.df_tp['birth'].apply(lambda x : 2015-x if x != 0 else np.random.randint(18,35) ) 
    
    def get_st_hist (self):

        if os.path.exists(self.st_hist_path):
            self.st_hist = pd.read_json(self.st_hist_path, encoding='utf8')
            return self.st_hist
        
        st_num , mth_num = self.st_date_range()
        st = datetime.date(*self.df_wt['wdate'][0])
        def mth_offset(x):
            date= datetime.date(*x)
            delta = dr.relativedelta( date,st)
            return delta.years*12 + delta.months        

        tmp_tp=self.df_tp
        tmp_tp['sdate']=tmp_tp['sdate'].apply(mth_offset)
        tmp_tp=tmp_tp.sort_values(by=['ssid','sdate'],axis=0)

        def slicing( df,attr,bins ):
            offset=np.cumsum(np.histogram(df[attr],bins=np.arange(bins+1))[0])
            sliced=[ df[:offset[i]] if i==0 else df[offset[i-1]:offset[i]] for i in range(len(offset)) ]
            return sliced
        tp_in_st = slicing( tmp_tp , 'ssid',st_num )

        table = self.attr_stat()
        bin_num = 20
        cols = []
        cols.extend(table.keys())
        cols.extend(['density'])
        cols.extend(['freq'])
        self.st_hist=pd.DataFrame(index= np.arange(st_num*mth_num), columns=cols)
        idx = 0

        tmp = self.df_tp.copy()
        tmp['birth']=tmp['birth'].apply(lambda x : math.floor(x/20))
        for x in tp_in_st:
            tp_mth = slicing(x , 'sdate' , mth_num)
            for y in tp_mth:
                for attr,conf in table.items():
                    hist_list = list()
                    hist_cols = ['gender', 'type', 'birth', 'events']
                    tmp_y = y.copy()
                    tmp_y['birth']=tmp_y['birth'].apply(lambda x : math.floor(x/20))
                    for col in hist_cols:
                        for event in tmp[col].unique():
                            hist_x = tmp_y[tmp_y[col]==event]
                            bins_arr = np.arange(conf[0],conf[1],(conf[1]-conf[0])/bin_num)
                            hist_x = hist_x.fillna(0)
                            hist = np.histogram(hist_x[attr], density=True, bins=bins_arr)[0]
                            hist_list.extend(hist)
                    self.st_hist[attr][idx] = hist_list
                    self.st_hist['freq'][idx] = len(tmp_y)/30.0
                    if len(tmp_y)>0:
                        self.st_hist['density'][idx] = tmp_y['density'].iloc[0]
                    else:
                        self.st_hist['density'][idx] = 0

                idx+=1

        for attr,conf in table.items():
            for i in range(len(self.st_hist[attr])):
                x = self.st_hist[attr][i]
                newx = [0 if v is None or np.isnan(v) else v for v in x]
                self.st_hist.set_value(i, attr, newx)

        self.st_hist.to_json(self.st_hist_path)

        return self.st_hist
            
    def attr_stat (self):
        attr_table = ['tp_dist','duration', 'meant', 'meanv']
        table = {}
        for attr in attr_table:
            std_val = self.df_tp[attr].std()
            min_val = max(self.df_tp[attr].mean() - 2*std_val, self.df_tp[attr].min())
            max_val = min(self.df_tp[attr].mean() + 2*std_val, self.df_tp[attr].max())
            table[attr] = [min_val, max_val, std_val]        
        return table

    def st_date_range(self):
        self.st_num = len(self.df_st)
        st = datetime.date(*self.df_wt['wdate'][0])
        ed = datetime.date(*self.df_wt['wdate'].iloc[-1])
        delta=dr.relativedelta( ed,st)
        self.mth_num = delta.years*12 + delta.months + 1
        return self.st_num , self.mth_num
    def st_freq_table ( self):
        st_num , mth_num = self.st_date_range()
        self.tp_cnt = np.zeros( (st_num,mth_num) ) 
        t= time.time()


        def mth_offset(x):
            date= datetime.date(*x)
            delta = dr.relativedelta( date,st)
            return delta.years*12 + delta.months
            
        for i , row in self.df_tp.iterrows():
            ssid= row['ssid']
            esid= row['esid']
            offset = mth_offset(row['sdate'])
            self.tp_cnt[ssid][offset] += 1
            self.tp_cnt[esid][offset] += 1
        
        self.tp_cnt/=60
        print (time.time() -t)
        t= time.time()


        freq = np.zeros( len(self.df_tp.index) )
        for i , row in self.df_tp.iterrows():
            offset=mth_offset(row['sdate'])
            sid = row['ssid'] if row['is_sst'] else row['esid']
            freq[i] = self.tp_cnt[sid][offset]
        print (time.time() -t)
        t= time.time()

       
        self.df_tp['freq'] = freq 
        self.savecomb_info()
    def save_comb_info(self):
        df_table = self.get_comb_info_table()
        t = time.time()
        combin_info = self.get_comb_info_from_table(df_table)
        combin_info.to_csv(self.comb_info_path, sep=',', encoding='utf-8', index=False)
        elapsed = time.time() - t
        print("elapsed time:", elapsed)
    def getcomb_info(self):
        if os.path.exists(self.comb_info_path):
            csvfile = pd.read_csv(self.comb_info_path, encoding='utf8')
            self.df_tp = pd.DataFrame(csvfile)
            table = ['sdate', 'stime', 'edate', 'etime']
            for attr in table:
                self.df_tp[attr] = self.df_tp[attr].apply(parsedstr2time)

    def save_station_csv(self, stime, etime):
        df_table = self.get_comb_info_table()
        sLength = len(self.df_st)
        df_st = self.df_st
        df_st['freq'] = np.zeros((sLength,1))
        for st_idx in range(len(self.df_st.index)):
            sid = self.df_st['sid'][st_idx]
            val = self.st_usage_freq(sid, stime, etime)
            df_st.set_value(st_idx, 'freq', val)
        df_st.to_csv(self.station_info_path, sep=',', encoding='utf-8', index=False)

class taskloader ():
    def __init__ (self,datasets):
        return
    def dataload(self):
        return
    def parse_data(self ,dataset='CY'):
        if dataset == 'CY':
            c = dl.CYShare()
            c.dataload(True)
            coll=datacollector(c)
            coll.getcomb_info()
            coll.df_tp['type']=coll.df_tp['type'].apply(lambda x : 1 if x=='Member' else -1 )
            coll.df_tp['gender']=coll.df_tp['gender'].apply(lambda x : {'Male':1 ,'Female':-1,'Other':0,'0':0}[x] )
        elif dataset == 'SF':
            s = dl.SFbay()
            s.dataload()
            coll=datacollector(s)
            coll.getcomb_info() 
            coll.df_tp['type']=coll.df_tp['type'].apply(lambda x : 1 if x=='Subscriber' else -1)
            coll.df_tp['birth'] = np.random.randint(22,30,size=len(coll.df_tp.index) ) 
            coll.df_tp['gender'] = np.random.randint(-1,2,size=len(coll.df_tp.index) ) 
        coll.df_tp['stime']=coll.df_tp['stime'].apply(lambda x: x[0])
        coll.df_tp['etime']=coll.df_tp['etime'].apply(lambda x: x[0])
        coll.df_tp['is_sst']= coll.df_tp['is_sst'].apply(lambda x : 1 if x else -1)
        coll.df_tp['events']=coll.df_tp['events'].apply(lambda x : 1 if x == 'Rain' else 0 ) 
        coll.df_tp = coll.df_tp.drop(self.droptable, axis=1)
        coll.df_tp =coll.df_tp.fillna(0)
        self.df = coll.df_tp
        self.coll = coll        
    def shuffle(self):
        self.y.shape = (self.y.shape[-1],1) 
        z = np.concatenate( (self.x,self.y) , axis= 1)
        np.random.shuffle(z)
        self.y = z[:,-1]
        self.x = z[:,:-1] 
        
        
class task1 ( taskloader):
    def __init__ (self):
        self.droptable=['tid','sdate','edate','bid','wdate']
        self.trainperc=0.5
        self.parseflag=0
        self.dir = './data/task1/'
        
    def parse (self):
        data = self.colls[0]
    def load_data(self,dataset='CY'):
        if ~self.parseflag:
            self.parse_data(dataset)
        self.y = self.df['freq'].as_matrix()
        self.x = self.df.drop(['freq'],axis=1).as_matrix ()
        self.shuffle()   
        sel_index = int(len(self.x)*self.trainperc)
        return ( self.x[:sel_index],self.y[:sel_index]),(self.x[sel_index:],self.y[sel_index:] )


class task1_hist ( taskloader):
    def __init__ (self):
        self.trainperc=0.8
        self.parseflag=0
        self.dir = './data/task1_hist/'
        
    def parse (self):
        data = self.colls[0]
    def load_data(self,dataset='CY'):
        c = dl.CYShare()
        c.dataload(True)
        self.coll=datacollector(c)
        self.coll.getcomb_info()
        st_hist = self.coll.get_st_hist()
        self.y = st_hist['freq'].as_matrix()
        x = st_hist.as_matrix (columns = ['duration','tp_dist','meanv','meant','density'])
        xx = np.array(x[:,0] + x[:,1]+ x[:,2]+ x[:,3])
        for i in range(len(xx)):
            xx[i] = np.append(xx[i], x[i,4])
        x = xx.tolist()
        self.x = np.array(x)
        self.shuffle()  
        sel_index = int(len(self.x)*self.trainperc)
        return ( self.x[:sel_index],self.y[:sel_index]),(self.x[sel_index:],self.y[sel_index:] )

class task_test(taskloader):
    def __init__(self):
        self.droptable=['tid','sdate','edate','bid','wdate']
        self.parseflag=0
        self.trainperc=0.5
    def load_data(self, dataset='CY'):
        if ~self.parseflag:
            self.parse_data(dataset)
        self.y=self.df['freq'].as_matrix()
        self.x=self.df.as_matrix(columns = ['duration','tp_dist'])

        #self.shuffle()
        sel_index = int(len(self.x)*self.trainperc)
        return ( self.x[:sel_index],self.y[:sel_index]),(self.x[sel_index:],self.y[sel_index:] )

def test():
    c = CYShare()
    c.dataload(True)
    coll = datacollector(c)
    coll.getcomb_info()
    return coll

if __name__ == '__main__':
    coll = test()
    st_hist = coll.get_st_hist()
    print(st_hist)


