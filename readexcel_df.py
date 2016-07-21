from collections import OrderedDict
import json
import pandas as pd

import pprint

class readExcel:

	def __init__(self, fileName, fileType, workSheet=None, col1=None, col2=None):
		self.fileName = fileName
		self.fileType = fileType
		self.workSheet = workSheet

		if (self.fileType == 'xls'):
			self.df = self.readXLSFile(self.fileName, workSheet, col1, col2)

		if (self.fileType == 'csv'):
			self.df = self.readCSVFile(self.fileName)



	def readCSVFile(self, fileName):
		pp = pprint.PrettyPrinter(indent=4)
		dfcsv = pd.read_csv(fileName)

#		pp.pprint(dfcsv)

		return dfcsv


	def readXLSFile(self, fileName, workSheet, col1, col2):

		pp = pprint.PrettyPrinter(indent=4)
		col_lst =[col1, col2]
		print col_lst
		rdxls = pd.read_excel(fileName, sheetname=workSheet)

		dfxls = pd.DataFrame(rdxls, columns=col_lst)
		pp.pprint(dfxls)

		return dfxls


	def getExcelData(self):
		return self.df

