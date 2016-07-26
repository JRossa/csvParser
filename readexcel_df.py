from collections import OrderedDict
import json
import pandas as pd
import sys
sys.path.insert(0, 'excelreader/')

from excelreader import *

import pprint

class readExcel:

	def __init__(self, fileName, fileType, workSheet=None,
				 columnsSearch=None, columnsPrint=None, config=None):
		self.fileName = fileName
		self.fileType = fileType
		self.workSheet = workSheet

		if (self.fileType == 'xls'):
			self.df = self.readXLSFile(self.fileName, workSheet, columnsSearch, columnsPrint)

		if (self.fileType == 'csv'):
			self.df = self.readCSVFile(self.fileName)

		if (self.fileType == 'xls_ine'):
			self.df = self.readXLSINEFile(self.fileName, workSheet, columnsSearch, columnsPrint, config)


	def readCSVFile(self, fileName):
		pp = pprint.PrettyPrinter(indent=4)
		dfcsv = pd.read_csv(fileName)

#		pp.pprint(dfcsv)

		return dfcsv


	def readXLSFile(self, fileName, workSheet, columnsSearch, columnsPrint):

		pp = pprint.PrettyPrinter(indent=4)

		print columnsSearch
		rdxls = pd.read_excel(fileName, sheetname=workSheet)

		if columnsSearch == None:
			dfxls = pd.DataFrame()
			dfxls = rdxls['indicadores']
		else:
			dfxls = pd.DataFrame(rdxls, columns=columnsSearch)
			dfxls.columns = columnsPrint

#		pp.pprint(dfxls)

		return dfxls


	def readXLSINEFile(self, fileName, workSheet, columnsSearch, columnsPrint, config):

		ine = ExcelReader(fileName, workSheet, config)

		ine.df = self.xlsINE2DataFrame(ine)

#		print (ine.df.columns)

		#select subset
		__sel_year = columnsSearch['date']
		__sel_local = columnsSearch['geo']
		__sel_data = columnsSearch['data']
		__sel_yearName = columnsPrint[0]

		if config['multi-index'] > 1:
			#select subset based on multi-indexes
			column_names_tuples = [(__sel_local, __sel_local),
								   (__sel_year, __sel_data)]
			ine.select_columns_from_df(column_names_tuples)

			ine.df = self.normalizeINEMultiIndex(ine.df, __sel_year, __sel_local, __sel_data, __sel_yearName)
		else:
			#select subset
			column_names = [__sel_local, __sel_year]
			ine.select_columns_from_df(column_names)

			ine.df = self.processINEDataFrame(ine.df, __sel_year, __sel_local, __sel_data, __sel_yearName)

		# 1. select rows with some data
		ine.df = ine.df[ine.df[__sel_data].notnull()]
		ine.df = ine.df[ine.df[__sel_local].notnull()]
		# 2. select the numerical info of localization
		ine.df = ine.df.reset_index(drop=True)
		# 2. set the headers names
		ine.df.columns = columnsPrint

#		print (ine.df)

		return ine.df


	def xlsINE2DataFrame(self, ine):
		#print ine.set_dataframe()
		ine.fill_down_column()

		ine.delete_rows()
		ine.df = ine.df.reset_index(drop=True)

		ine.fill_right_row()

		#define multi-indexes on rows. In this case only first row will be considered
		ine.set_multi_indexed_rows()

		ine.df = ine.df.where((pd.notnull(ine.df)), None)

		#remove diacritics
		ine.df = ine.df.rename(columns=lambda x: ine.rename_column(x))

		return ine.df


	def normalizeINEMultiIndex(self, inedf, __sel_year, __sel_local, __sel_data, __sel_yearName):
		# Normalize multi-indexes dataframe
		# 1. build - dfNew
		dfNew = pd.DataFrame(inedf[__sel_year])
		# 2. rename valdf 2nd column to be deleted
		dfNew.columns = [__sel_data, 'annotations']
		# 3. delete extra info column
		dfNew.drop('annotations', axis=1, inplace=True)
		# 4. insert location column into dfNew
		dfNew[__sel_local] = inedf[__sel_local]
		# 5. insert year column into dfNew
		dfNew[__sel_yearName] = pd.Series([0 for x in range(len(dfNew.index))])
		# 6. fill with the value
		dfNew[__sel_yearName] = __sel_year

		cols = dfNew.columns.tolist()
		# 7. rearange columns to the customize order (date, geo, value)
		#    reverse list (cols.reverse() ->  doesn't work)
		cols = cols[::-1]
		dfNew =dfNew[cols]

		dfNew[__sel_local] = self.processDDCCFFCode(dfNew[__sel_local])

		return dfNew


	def processINEDataFrame(self, inedf, __sel_year, __sel_local, __sel_data, __sel_yearName):
		# 1. build - dfNew
		dfNew = pd.DataFrame(inedf)

		# 1. rename 3rd column to be deleted
		dfNew.columns = [__sel_local, __sel_data, 'annotations']
		# 2. delete extra info column
		dfNew.drop('annotations', axis=1, inplace=True)
		# 3. insert year column into ine.df
		dfNew[__sel_yearName] = pd.Series([0 for x in range(len(dfNew.index))])
		# 4. fill with the value
		dfNew[__sel_yearName] = __sel_year
		# 5. rearange columns order to the customize order (date, geo, value)
		cols = dfNew.columns.tolist()
		cols = cols[-1:] + cols[:-1]

		dfNew = dfNew[cols]

		dfNew[__sel_local] = self.processDDCCFFCode(dfNew[__sel_local])

		return dfNew


	def processDDCCFFCode(self, columnDDCCFF):
		for ind, place in columnDDCCFF.iteritems():
			a = place.split(':')

			if len(a[0]) > 6:
				columnDDCCFF.ix[ind] = a[0][len(a[0])-6:]
			else:
				columnDDCCFF.ix[ind] = None

		return columnDDCCFF


	def getExcelMetaData(self, fileName, workSheet, config):

		ine = ExcelReader(fileName, workSheet, config)

		ine.df = self.xlsINE2DataFrame(ine)

#		print ine.df.columns
		return list(set(ine.df.columns))

	def getExcelData(self):
		return self.df

