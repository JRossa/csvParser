from collections import OrderedDict
import json
import csv

class readProcessor:

	def __init__(self, fileName, fileType):
		self.fileName = fileName
		self.fileType = fileType

		self.column = OrderedDict()

		if (self.fileType == 'json'):
			self.column = self.readJSONFile(self.fileName)

		if (self.fileType == 'csv'):
			self.column = self.readCSVFile(self.fileName)


	# Read csv fileName data, for header and data
	def readCSVFile(self, fileName):

		column = OrderedDict()

		with open(fileName, 'r') as f:
			f.seek(0)  # rewind
			reader = csv.reader(f)

			headers = next(reader)

			column = {h:[] for h in headers}
			for row in reader:
				for h, v in zip(headers, row):
					column[h].append(v)

		return column

	def readJSONFile(self, fileName):	

		column = OrderedDict()

		# Reading data back
		with open(fileName, 'r') as f:
			data = json.load(f)


		for dt in data:
			if (dt == 'processor'):
				print('---------- processor ---------------')
#				print(data[dt])
				headers = []
				rowt = {}
				for att in data[dt]:
					dLst = list(att.keys())
					rowt = {h:[] for h in dLst}
					break

				for att in data[dt]:
					headers.append(att['tag'])
					for ii, attitem in zip(dLst, att):
						rowt[ii].append(att[attitem])

				reader = []
				reader.append(rowt['type'])
				reader.append(rowt['table'])
				reader.append(rowt['value'])
				reader.append(rowt['pkey'])
				reader.append(rowt['fkey'])

				column = {h:[] for h in headers}
				for row in reader:
					for h, v in zip(headers, row):
						column[h].append(v)

#			print(headers)

			if (dt == 'locale'):
				print('---------- locale ---------------')
				print(data[dt])

			if (dt == 'cubes'):
				print('---------- cubes ---------------')
				cubesData = data[dt]
				for ccdd in cubesData:
					print('ft_' + ccdd['name']) 
					ccJoins = ccdd['joins']
					for ccjj in ccJoins:
						print(ccjj['detail'])
						print(ccjj['master'])

			if (dt == 'dimensions'):
				print('---------- dimensions ---------------')
				dimData = data[dt]
				for dddd in dimData:
					print('dm_' + dddd['name']) 

#		print(column)
		return column


	def getProcessorData(self):
		return self.column

