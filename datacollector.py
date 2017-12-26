import os
import math
import time
import datetime
import numpy as np
import pandas as pd
from timehelper import *

class  datacollector():
    """docstring for  datacollector"""
    def __init__(self, infos):
        df_st = pd.DataFrame(infos[0]['st'])
        df_dist_st = self.get_station_distinfo(df_st)
        self.df_st = pd.concat([df_st, df_dist_st], axis=1)
        
        self.df_tp = pd.DataFrame(infos[0]['tp'])
        self.df_wt = pd.DataFrame(infos[0]['wt'])
        
        o_dir = infos[1]
        self.table_path = os.path.join(o_dir, 'combin_info_table.csv')
        self.comb_info_path = os.path.join(o_dir, 'combin_info.csv')

    def distance(self, p0, p1):
        return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)     

    def get_station_distinfo(self, df_st):
        # 1.calculate the distance between stations
        # 2. record the min distance between stations
        points = list(zip(df_st['lat'], df_st['long']))
        st_num = sum(1 for x in points)
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
        
        df_dist_info = pd.DataFrame(index = range(len(df_st)))
        df_dist_info['mindist'] = min_dist
        df_dist_info['mindist_sid'] = min_station

        for i in range(st_num):
            newcolname = 'dist2_' + df_st['sid'][i]
            df_dist_info[newcolname] = points_dist[i,:]
        
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
        df_tp_wt = pd.DataFrame(columns=list(self.df_wt))
        df_tp_st = pd.DataFrame(columns=list(self.df_st))
        print(len(df_table.index))
        for i in range(len(df_table.index)):
            w_idx = int(df_table['wt_idx'][i])
            s_idx = int(df_table['st_idx'][i])
            df_tp_wt = df_tp_wt.append(self.df_wt.iloc[w_idx].copy())
            df_tp_st = df_tp_st.append(self.df_st.iloc[s_idx].copy())
            if(i%1000==0):
                print(i)

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

        with open(self.comb_info_path,'w') as f:
            for j in range(len(list(df_table))):
                attr = str(list(df_table)[j]) + ','
                f.write(attr)
            for j in range(len(list(self.df_wt))):
                attr = str(list(self.df_wt)[j]) + ','
                f.write(attr)
            for j in range(len(list(self.df_st))):
                if j == len(list(self.df_st))-1:
                    attr = str(list(self.df_st)[j]) + '\n'
                else:
                    attr = str(list(self.df_st)[j]) + ','
                f.write(attr)
            
            for i in range(len(df_table.index)):
                w_idx = int(df_table['wt_idx'][i])
                s_idx = int(df_table['st_idx'][i])
                for j in range(len(list(df_table))):
                    if type(df_table.iloc[i][j]) == list:
                        attr = '"' + str(df_table.iloc[i][j]) + '"' + ','
                    else:
                        attr = str(df_table.iloc[i][j]) + ','
                    f.write(attr)
                for j in range(len(list(self.df_wt))):
                    if type(self.df_wt.iloc[w_idx][j]) == list:
                        attr = '"' + str(self.df_wt.iloc[w_idx][j]) + '"' + ','
                    else:
                        attr = str(self.df_wt.iloc[w_idx][j]) + ','
                    f.write(attr)
                for j in range(len(list(self.df_st))):
                    if j == len(list(self.df_st))-1:
                        attr = str(list(self.df_st.iloc[s_idx])[j]) + '\n'
                    else:
                        attr = str(list(self.df_st.iloc[s_idx])[j]) + ','
                    f.write(attr)                
                if(i%1000==0):
                    print(i)       
