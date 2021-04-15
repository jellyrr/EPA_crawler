
from multiprocessing import Pool, cpu_count
import pickle as pkl
import subprocess as sub
from pandas import date_range, read_csv, concat, DataFrame
from os.path import join as pth, exists, realpath
from os import listdir, mkdir
from datetime import datetime as dtm
import numpy as n
from functools import partial
from pathlib import PureWindowsPath

## api web
## https://data.epa.gov.tw/api/v1
## open data web
## https://data.epa.gov.tw/

## 1. get the county air pollution data
## 1. select wanted station name

## set the selection : if want whole data, u have to get all data and select wanted station

## bug box
"""


"""



## time decorater
def __timer(func):
	def __wrap():
		
		__st = dtm.now().timestamp()

		## main function
		func()

		__fn = dtm.now().timestamp()
		__run = (__fn-__st)/60.

		print(f'\nProgram done\nrunning time = {int(__run):3d} min {(__run-int(__run))*60.:6.3f} s')

	return __wrap


	runtime = (__final-__start)/60.
	print(f'\nProgram done\nrunning time = {int(runtime):3d} min {(runtime-int(runtime))*60.:6.3f} s')


## use bash file to download file
def bash_curl(_off,nam,num,yr_mon,dir_nam):

	## parameter
	fname = f'{nam}_{yr_mon}_off{_off:06d}.csv'

	## judgment the file exists or not
	## shut down the dowload process
	if exists(pth(dir_nam,fname)): return True

	## run the download bash
	print(f'\r\t\tdownload : {fname}')
	sub.run(['bash','crawl.sh',PureWindowsPath(dir_nam).as_posix(),fname,num,str(_off),yr_mon])

	return True if exists(pth(dir_nam,fname)) else False


## main run
@__timer
def run():

	print(f"Download EPA data from EPA api web (\33[94mhttps://data.epa.gov.tw/api\33[0m)")
	print(f"{dtm.now().strftime('%Y/%m/%d %X')}")
	print('='*65)

	## station number
	with open(pth('station_api.pkl'),'rb') as f:
		station = pkl.load(f)

	_compl = input('\nGet completed EPA station data(with meteorological parameter) ?(y/n) ')
	
	## get input information
	re = True
	while re:
		try:
			if _compl!='n':

				## complete data
				## download whole station data and get selected station
				## 鹿林山站 is background station, but it 
				sta_index = input('Station Location(in Chinese Ex. \u81fa\u4e2d\u5e02) : ')
				sta_nam	  = input('Station Name(in Chinese Ex. \u5fe0\u660e) : ')

				sta_num   = station[sta_nam] if sta_index=='\u9e7f\u6797\u5c71' else station[sta_index]

			else:
				## data without meteorological parameter
				## download selective station data
				sta_nam = input('Station Name(in Chinese Ex. \u5fe0\u660e) : ')
				sta_num = station[sta_nam]
			re = False

		except KeyError:
			print(f'\n\33[91mStation name is not in EPA station list, please enter again !\33[0m\n')
			re = True

	

	## delete 站 in sta_nam
	sta_nam = sta_nam[:-1] if '\u7ad9' in sta_nam else sta_nam


	## date range
	st_tm = dtm.strptime(input(f'Start date(YYYY/MM/DD) : '),'%Y/%m/%d')
	ed_tm = dtm.strptime(input(f'  End date(YYYY/MM/DD) : '),'%Y/%m/%d')

	## out dir
	out_dir = input('Output file path(default is ../data) : ')
	out_dir = out_dir if out_dir is not '' else pth('..','data')

	## parameter
	## set time list
	period   = (ed_tm.year-st_tm.year)*12+(ed_tm.month-st_tm.month)+1
	date_lst = list(date_range(st_tm,freq='1M',periods=period).strftime('%Y_%m'))

	## make dir and download data
	dir_nam = f"{sta_nam}_{date_lst[0].replace('/','')}-{date_lst[-1].replace('/','')}"
	out_pth = pth(out_dir,dir_nam)
	mkdir(out_pth) if not exists(out_pth) else None

	# return
	## main
	## download
	print(f'\n{"-"*20} donwload by {cpu_count():2d} cpu {"-"*20}')
	for _time in date_lst:
		offset = n.arange(0,1000*(cpu_count()+1),1000)

		print(f'\n\tread {_time} file')
		_stop_comm = [True]

		while False not in _stop_comm:
			pool = Pool(cpu_count())
			
			try:
				_stop_comm = pool.map(partial(bash_curl,nam=sta_nam,num=sta_num,yr_mon=_time,dir_nam=out_pth),offset)
			except Exception as exc:
				print('\tbash has wrong, followiing are some Exception')
				print(exc.__class__)
				break

			offset += 1000*(cpu_count()+1)

			pool.close()
			pool.join()

		pool.close()
		pool.join()
		print()

	## read file and output the processing file
	print(f'\n{"-"*60}\n')
	print('\nRebuild the file to complete csv file')
	flist = []
	for file in listdir(out_pth):
		if '.csv' not in file: continue
		print(f'\r\tread {file}',end='')

		with open(pth(out_pth,file),'r',encoding='utf-8',errors='ignore') as f:
			_dt = read_csv(f,parse_dates=['MonitorDate'],na_values=['x']).set_index('MonitorDate')
			flist.append(_dt.loc[_dt.SiteName==sta_nam])
	print()

	df_dict = {}
	dt = concat(flist).groupby('ItemEngName')
	for nam, _df in dt:
		col_nam = f'{nam} ({_df.ItemUnit[0]})'
		df_dict[col_nam] = _df[~_df.index.duplicated(keep='first').copy()].reindex(date_range(st_tm,ed_tm,freq='1h')).Concentration
	fout = DataFrame(df_dict)

	fout.to_csv(pth(out_dir,f'{sta_nam}_{st_tm.strftime("%Y%m%d")}_{ed_tm.strftime("%Y%m%d")}.csv'))







if __name__ == "__main__":




	print(f'\nPROGRAM : {__file__}\n')

	run()


