import os
import math
import time
import datetime
import numpy as np
import pandas as pd
from timehelper import *
import time

class  datacollector():
    """docstring for  datacollector"""
    def __init__(self, dl):
        df_st = pd.DataFrame(dl.dt['st'])
        self.st2IdTable = { x:df_st['sid'][x] for x in df_st.index }
        self.id2StTable = { df_st['sid'][x] :x for x in df_st.index}
        df_dist_st = self.get_station_distinfo(df_st)
        self.df_st = pd.concat([df_st, df_dist_st], axis=1)
        
        self.df_tp = pd.DataFrame(dl.dt['tp'])
        self.df_wt = pd.DataFrame(dl.dt['wt'])
        
        o_dir = dl.dir
        self.table_path = os.path.join(o_dir, 'combin_info_table.csv')
        self.comb_info_path = os.path.join(o_dir, 'combin_info_2.csv')
        self.station_info_path = os.path.join(o_dir, 'station_info.csv')

    def distance(self, p0, p1):
        return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)     

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
                    min_station[i] = df_st['sid'][j]
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

    def get_tps_correlate2st(self, sid, isStart):
        col = 'ssid' if isStart else 'esid'
        df_tp_sub = self.df_tp[self.df_tp[col]==sid].copy()
        tp_sub_num = len(df_tp_sub.index)
        df_tp_sub.index = range(tp_sub_num)
        if tp_sub_num > 0:
            df_tp_sub['tp_dist'] = np.ones((tp_sub_num,1))
            if isStart:
                df_tp_sub['is_sst'] = np.ones((tp_sub_num,1))
            else:
                df_tp_sub['is_sst'] = np.zeros((tp_sub_num,1))
        return df_tp_sub

    def attach_wt_st_info(self, df_tp, st_idx, isStart):
        rm_row = []
        col = 'esid' if isStart else 'ssid'
        for i in range(len(df_tp)):
            stid = df_tp[col].iloc[i]
            colsname = 'dist2_' + stid
            if colsname in list(self.df_st):
                dist = self.df_st[colsname].iloc[st_idx]
                df_tp.set_value(i, 'tp_dist', dist)
                
                time = df_tp['sdate'].iloc[i]
                wt_idx = timesel(self.df_wt, 'wdate', time).index[0]
                df_tp.set_value(i, 'wt_idx', wt_idx)
                df_tp.set_value(i, 'st_idx', st_idx)
            else:
                rm_row.append(i)
        if rm_row:
            df_tp = df_tp.drop(df_tp.index[rm_row])

        return df_tp

    def get_comb_info_table(self, save=True):

        if os.path.exists(self.table_path):
            csvfile = pd.read_csv(self.table_path, encoding='utf8')
            self.comb_table = pd.DataFrame(csvfile)
            table = ['sdate', 'stime', 'edate', 'etime']
            for attr in table:
                self.comb_table[attr] = self.comb_table[attr].apply(parsedstr2time)
            return self.comb_table

        task_cols = list(self.df_tp)
        task_cols.extend(['tp_dist'])
        task_cols.extend(['wt_idx'])
        task_cols.extend(['st_idx'])

        df_comb_table = pd.DataFrame(columns=task_cols)
        df_comb_table = df_comb_table.fillna(0)
 
        # trips corresponding to certain station
        for st_idx in range(len(self.df_st.index)):
            sid = self.df_st['sid'][st_idx]
            print(st_idx, sid)

            df_stp = self.get_tps_correlate2st(sid, isStart=True)
            df_etp = self.get_tps_correlate2st(sid, isStart=False)
            
            df_stp = self.attach_wt_st_info(df_stp, st_idx, isStart=True)
            df_etp = self.attach_wt_st_info(df_etp, st_idx, isStart=False)

            stp_num = len(df_stp.index)
            etp_num = len(df_etp.index)


            if stp_num + etp_num > 1:
                df_setp = pd.concat([df_stp, df_etp])
            elif stp_num + etp_num == 1:
                df_setp = df_stp if stp_num ==1 else df_etp
            else:
                continue

            df_setp.index = range(len(df_setp.index))
            df_comb_table = df_comb_table.append(df_setp)

        if save:
            df_comb_table.to_csv(self.table_path, sep=',', encoding='utf-8', index=False)

        self.comb_table = df_comb_table
        return self.comb_table

    def get_comb_info_from_table(self, df_table, save=False):        
        print('start')
        df_tp_wt = df_table['wt_idx'].apply(lambda x: self.df_wt.iloc[int(x)].copy() )
        df_tp_st = df_table['st_idx'].apply(lambda x: self.df_st.iloc[int(x)].copy() )
        print('check') 
        df_tp_wt.index = df_table.index
        df_tp_st.index = df_table.index
        df_comb_info = pd.concat([df_table, df_tp_wt], axis=1)
        df_comb_info = pd.concat([df_comb_info, df_tp_st], axis=1)
        return df_comb_info

    def st_usage_freq(self, sid, stime, etime):
        df_intime = timesel_interval(self.comb_table, 'sdate' ,stime, etime)

        while(len(stime))<3:
            stime.append(1)
        while (len(etime))<3:
            etime.append(1)
        sday = datetime.date(stime[0],stime[1],stime[2])
        eday = datetime.date(etime[0],etime[1],etime[2])
        diff = eday - sday
        days = float(diff.days)
        sn = float(len(df_intime[df_intime['ssid'] == sid].index)/2)
        en = float(len(df_intime[df_intime['esid'] == sid].index)/2)
        freq = (sn+en) / days
        return freq

    def load_task1_data(self, stime, etime):
        df_table = self.get_comb_info_table()

        print(len(df_table.index))
        df_table_intime = timesel_interval(df_table, 'sdate' ,stime, etime)
        df_table_intime.index = range(len(df_table_intime.index))
        print(len(df_table_intime.index))

        df_comb_info = self.get_comb_info_from_table(df_table)
        df_comb_info.to_csv(self.comb_info_path, sep=',', encoding='utf-8', index=False)
        
        freq_table = {}
        for st_idx in range(len(self.df_st.index)):
            sid = self.df_st['sid'][st_idx]
            freq_table[sid] = self.st_usage_freq(sid, stime, etime)
            print(sid)

        target = []
        print(list(df_intime))

    def save_comb_info(self):
        df_table = self.get_comb_info_table()
        print(len(df_table))
        t = time.time()
        combin_info = self.get_comb_info_from_table(df_table)
        combin_info.to_csv(self.comb_info_path, sep=',', encoding='utf-8', index=False)
        elapsed = time.time() - t
        print("elapsed time:", elapsed)

    def save_station_csv(self, stime, etime):
        df_table = self.get_comb_info_table()
        sLength = len(self.df_st)
        freq = pd.Series(np.random.randn(sLength))
        for st_idx in range(len(self.df_st.index)):
            sid = self.df_st['sid'][st_idx]
            val = self.st_usage_freq(sid, stime, etime)
            freq[st_idx] = val


        df_st = pd.concat([self.df_st, freq], axis=1)
        df_st.to_csv(self.station_info_path, sep=',', encoding='utf-8', index=False)
