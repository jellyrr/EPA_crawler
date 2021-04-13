
from multiprocessing import Pool, cpu_count
import pickle as pkl
import subprocess as sub
from pandas import date_range, read_csv, concat, DataFrame
from os.path import join as pth, exists
from os import listdir, mkdir
from datetime import datetime as dtm
import numpy as n
from functools import partial

## api web
## https://data.epa.gov.tw/api/
## open data web
## https://data.epa.gov.tw/

## 1. get the county air pollution data
## 1. select wanted station name

## set the selection : if want whole data, u have to get all data and select wanted station

## 
def bash_curl(_off,nam,num,yr_mon,dir_nam):

	## parameter
	apiKey = '8decfe1b-6c94-4f04-8fa7-f64c9652daf4'
	fname = f'{nam}_{yr_mon}_{_off:06d}.csv'

	## judgment the file exists or not
	## shut down the dowload process
	if exists(pth(dir_nam,fname)): return True

	## run the download bash
	sub.run(['bash','crawl.sh',dir_nam,fname,num,str(_off),yr_mon,apiKey])

	return True if exists(pth(dir_nam,fname)) else False


if __name__ == "__main__":

	## station number
	with open(pth('station_api.pkl'),'rb') as f:
		station = pkl.load(f)

##
	# countyNam = input('Station location(in Chinese Ex. 臺中市) : ')
	# staNam = input('Station Name(in Chinese Ex. 忠明) : ')
	# staNum = station[countyNam]

	staNam = input('Station Name(in Chinese Ex. 忠明站) : ')
	# staNam = input('Station Name(in Chinese Ex. 忠明) : ')
	staNum = station[staNam]
##

	## date range
	stTm = dtm.strptime(input(f'Start date(YYYY/MM/DD) : '),'%Y/%m/%d')
	edTm = dtm.strptime(input(f'  End date(YYYY/MM/DD) : '),'%Y/%m/%d')

	period = (edTm.year-stTm.year)*12+(edTm.month-stTm.month)+1
	dateLst = list(date_range(stTm,freq='1M',periods=period).strftime('%Y_%m'))

	## make dir and download data
	dirNam = f"{staNam}_{dateLst[0].replace('/','')}-{dateLst[-1].replace('/','')}"
	outPath = pth('.',dirNam)
	mkdir(outPath) if not exists(outPath) else None

	## download
	for _time in dateLst:
		offset = n.arange(0,1000*(cpu_count()+1),1000)

		print(f'read {_time} file')
		_stop_comm = [True]

		while False not in _stop_comm:
			pool = Pool(cpu_count())
			
			try:
				_stop_comm = pool.map(partial(bash_curl,nam=staNam,num=staNum,yr_mon=_time,dir_nam=dirNam),offset)
			except Exception as exc:
				print('bash has wrong, followiing are some Exception')
				print(exc.__class__)
				break

			offset += 1000*(cpu_count()+1)

			pool.close()
			pool.join()

		pool.close()
		pool.join()

	## read file and output the processing file
	print('\nrebuild the file')
	fList = []
	for file in listdir(outPath):
		if '.csv' not in file: continue
		print(f'\rread {file}',end='')

		with open(pth(outPath,file),'r',encoding='utf-8',errors='ignore') as f:
			_dt = read_csv(f,parse_dates=['MonitorDate'],na_values=['x']).set_index('MonitorDate')
##
			# fList.append(_dt.loc[_dt.SiteName==staNam])
			fList.append(_dt)
##


	print()

	df_dict = {}
	dt = concat(fList).groupby('ItemEngName')
	for nam, _df in dt:
		col_nam = f'{nam} ({_df.ItemUnit[0]})'
		df_dict[col_nam] = _df[~_df.index.duplicated(keep='first').copy()].reindex(date_range(stTm,edTm,freq='1h')).Concentration
	fout = DataFrame(df_dict)


	fout.to_csv(pth(f'{staNam}_{stTm.strftime("%Y%m%d")}_{edTm.strftime("%Y%m%d")}.csv'))


	# '''









