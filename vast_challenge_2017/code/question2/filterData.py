import pandas as pd
import numpy as np
import shutil
import os
import datetime

def create_empty_folder(dir='output'):
	if os.path.exists(dir):
		shutil.rmtree(dir)
	os.makedirs(dir)

def readDataset(dataset):
		print('Reading input data from',dataset)
		X=pd.read_csv(dataset)
		#X.columns = ['Timestamp','car_id','car_type','gate_name']
		X.columns=['car_id','car_type','gate_name','Timestamp','togate','totime','timediff']

		N , D = X.shape
		X['Timestamp']=pd.to_datetime(X['Timestamp'])
		X['Time']=[X.loc[i,'Timestamp'].time() for i in range(X.shape[0])]
		X['Day']=[X.loc[i,'Timestamp'].weekday() for i in range(X.shape[0])] #0 : monday, 1 : tuesday,.... 6 : Sunday
		print ('No of data points :',N,'\nNo of dimensions',D)
		return X
def readLekagul(dataset):
		print('Reading input data from',dataset)
		X=pd.read_csv(dataset)
		X.columns = ['Timestamp','car_id','car_type','gate_name']
		N , D = X.shape
		X['Timestamp']=pd.to_datetime(X['Timestamp'])
		X['Time']=[X.loc[i,'Timestamp'].time() for i in range(X.shape[0])]
		X['Day']=[X.loc[i,'Timestamp'].weekday() for i in range(X.shape[0])] #0 : monday, 1 : tuesday,.... 6 : Sunday
		print ('No of data points :',N,'\nNo of dimensions',D)
		return X


def weekwiseanalysis(X):
	weekday=[0,1,2,3,4,5,6]
	gates=['camping0','camping1','camping2','camping3','camping4','camping5','camping6','camping7','camping8']
	obj=datafilter()
	for i in range(len(gates)):
		obj.setGateName(list(gates[i]))
		for j in range(len(weekday)):
			print(gates[i],weekday[j])
			obj.setWeekday(list(str(weekday[j])))
			tmp=obj.runFilter(X)
		print(tmp)
		break



class datafilter:

	def __init__(self):
		self.car_type=[]
		self.car_id=[]
		self.gate_name=[]
		self.startDateTime=''
		self.endDateTime=''
		self.weekday=[]
		self.start_date=''
		self.end_date=''
		self.start_time=''
		self.end_time=''
		self.month=[]
		self.year=[]

	def setCarId(self,car_id):
		self.car_id=car_id

	def setCarType(self,car_type):
		self.car_type=car_type

	def setGateName(self,gate_name):
		self.gate_name=gate_name

	def setStartDate(self,start_date):
		self.start_date=start_date

	def setEndDate(self,end_date):
		self.end_date=end_date


	def setStartTime(self,start_time):
		self.start_time=start_time

	def setEndTime(self,end_time):
		self.end_time=end_time

	def setWeekday(self,weekday):
		self.weekday=weekday

	def setMonth(self,month):
		self.month=month

	def setYear(self,year):
		self.year=year		
       
	"""
	def printfilter(self):
		if(self.car_type==[]):
			print('All car type')
		else:
			print('cartype :',self.car_type)
		if(self.weekday==[]):
			print('ALl weekday')
		else:
			print('weekday',self.weekday`) 
	"""
	
	def runFilter(self,X):
		
		if(self.car_id):
			X=X.loc[X['car_id'].isin(self.car_id)]
		if(self.gate_name!=[]):
			X=X.loc[X['gate_name'].isin(self.gate_name)]
		if(self.car_type!=[]):
			X=X.loc[X['car_type'].isin(self.car_type)]
		if(self.start_date):
			X=X[(X['Timestamp']>=self.start_date)]
		if(self.end_date):
			X=X[(X['Timestamp']<=self.end_date)]
		if(self.end_time):
			X=X[(X['Time']<=self.end_time)]
		if(self.start_time):
			X=X[(X['Time']>=self.start_time)]
		if(self.weekday):
			X=X[X['Day'].isin(self.weekday)]
		if(self.month):
			print('month is',self.month)
			X=X[X['Month'].isin(self.month)]
		if(self.year):
			X=X[X['Year'].isin(self.year)]
		return X

	def getAllPath(self,X):
		set1=set()
		for row in range(X.shape[0]):
			set1.add((X.iloc[row,2],X.iloc[row,4]))
		pathlist=pd.DataFrame(list(set1),columns=['source','destination'])
		#pathlist.to_csv('allPossiblePaths.csv',index=False)
		print('pathlist')
		print(pathlist)
		return pathlist


if __name__=='__main__':
	params={}
	params['dataset']='./static/data/Lekagul_Sensor_Data_less.csv'
	#params['dataset']='processed_output_allData.csv'
	#create_empty_folder('../output')
	#obj=dp.DataPreprocessing(params)
	#X=obj.readDataset()
	#X=readDataset(params['dataset'])
	X=readLekagul(params['dataset'])
	obj=datafilter()
	X['Timestamp']=pd.to_datetime(X['Timestamp'])
	X['Day']=[X.loc[i,'Timestamp'].weekday() for i in range(X.shape[0])]
	X['Month']=[X.loc[i,'Timestamp'].month for i in range(X.shape[0])]
	X['Year']=[X.loc[i,'Timestamp'].year for i in range(X.shape[0])]
	obj.setMonth([6])
	obj.setYear([2015])
	#obj.setCarId(['20154301124328-262','20155201025212-846','20153701033700-323'])
	#obj.setCarType(['1','2','3','4','5','6'])
	#obj.setGateName(['ranger-stop1'])
	#obj.setStartDateTime(datetime.datetime(2015,8,1,1,1,1))
	#obj.setEndDateTime(datetime.datetime(2015,9,1,1,1,1))
	#obj.setStartDate(datetime.date(2015,8,1))
	#obj.setEndDate(datetime.date(2015,8,8))
	#obj.setWeekday([6])
	#obj.setStartTime(datetime.time(10,0,0))
	#obj.setEndTime(datetime.time(11,0,0))
	X=obj.runFilter(X)
	print(X)
	#weekwiseanalysis(X)
	#X.to_csv('./static/data/processed_days.csv',index=False,header=None)
	
	
