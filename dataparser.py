import csv
import math
import numpy as np
import pandas as pd
station_csv_path = './datasets/cycle-share-dataset/station.csv'
trip_csv_path = './datasets/cycle-share-dataset/trip.csv'
weather_csv_path = './datasets/cycle-share-dataset/weather.csv'

def distance(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)

def station_parser(station_csv_path):
	
	csvfile = pd.read_csv(station_csv_path)
	df = pd.DataFrame(csvfile)
	# remove the unused cols
	rmcols = ["name","install_date","install_dockcount","modification_date","decommission_date"]
	for col in rmcols:
		df = df.drop(col, 1)

	# remove the rows which has nan
	df = df.dropna()

	# calculate the distance between stations and record the min
	# distance between stations
	points = zip(df['lat'], df['long'])
	station_num = len(points)
	points_dist = np.zeros((station_num, station_num))
	min_dist = np.ones((station_num,1))
	min_station = ["" for x in range(station_num)]
	for i in range(station_num):
		for j in range(station_num):
			dist = distance(points[i],points[j])
			points_dist[i,j] = dist
			if (i != j) and (dist < min_dist[i]):
				min_dist[i] = dist
				min_station[i] = df['station_id'][j]
	
	df['min_dist'] = min_dist
	df['min_station'] = min_station

	# add the distance information to the dataframe
	for i in range(station_num):
		newcolname = 'dist_to_' + df['station_id'][i]
		df[newcolname] = points_dist[i,:]
	
	# calculate the density of the station (staion numbers in average distance)
	avg_dist = np.mean(points_dist)
	density = np.zeros((station_num,1))
	for i in range(station_num):
		density[i] = len(np.where(points_dist[0,:] < avg_dist)[0])-1
	
	df['density'] = density

	return df


df_station = station_parser(station_csv_path)