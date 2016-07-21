from collections import OrderedDict
import json
import pandas as pd

import pprint

class readProcessor:

	def __init__(self, fileName, fileType, ind=None):
		self.fileName = fileName
		self.fileType = fileType

		if (self.fileType == 'json'):
			self.df = self.readJSONFile(self.fileName, ind)

		if (self.fileType == 'csv'):
			self.df = self.readCSVFile(self.fileName)



	def readCSVFile(self, fileName):
		pp = pprint.PrettyPrinter(indent=4)
		dfcsv = pd.read_csv(fileName)

#		pp.pprint(dfcsv)
		# rename the index values
		dfcsv = dfcsv.rename(index={0: 'type', 1:'table', 2: 'value', \
								    3: 'pkey', 4: 'fkey'})

		return dfcsv


	def readJSONFile(self, fileName, ind):
		pp = pprint.PrettyPrinter(indent=4)

		# read json file - series (dataframe doesn't work)
		sjson = pd.read_json(fileName, typ='series')

		# start process json file
		dfc = pd.DataFrame(sjson['cubes'])

#		pp.pprint ("DF - Cubes")
#		pp.pprint (len(dfc))

		# read cubes features data - ft
		for cc in range(len(dfc)):
			_ft = 'ft_' + dfc.ix[cc, ['name']][0]
			_value = dfc.ix[cc, ['processor']][0]['value']
			_pkey = dfc.ix[cc, ['processor']][0]['pkey']
			ft_lst = [u'ft', _ft, _value, \
					  _pkey, u'none']

			# stores dm list of lists
			dm_lst = []
			dfd = pd.DataFrame(sjson['dimensions'])

			# for each fact read the dimensions data - dm
			for dd in range(len(dfd)):
				_dm = 'dm_' + dfd.ix[dd, ['name']][0]
				_value = dfd.ix[dd, ['processor']][0]['value']
				_pkey = dfd.ix[dd, ['processor']][0]['pkey']
				_fkey = dfd.ix[dd, ['processor']][0]['fkey']
				dm_lst.append([u'dm', _dm, _value, _pkey, _fkey])

			# start building output DataFrame
			jdf = pd.DataFrame()

			# insert the dimension series (dm) to the output DataFrame
			for dm in dm_lst:
#				pp.pprint (dm)
				df_t = pd.DataFrame(dm, columns=[dm[2]])
				jdf = pd.concat([jdf, df_t], join='outer', axis=1)

			# join - outer => union
			#        inner => interception
			# insert the fact series (ft) to the output DataFrame
			df_t = pd.DataFrame(ft_lst, columns=[ft_lst[2]])
			jdf = pd.concat([jdf, df_t], join='outer', axis=1)

			# rename the index values
			jdf = jdf.rename(index={0: 'type', 1:'table', 2: 'value', \
									3: 'pkey', 4: 'fkey'})

#			pp.pprint (jdf)
#			pp.pprint (jdf.loc['value', 'freguesia_code'])

#			dict = pd.DataFrame.to_dict(jdf, orient=str('list'))
#			pp.pprint (dict)

			# select the ind ft
			if cc == ind:
				break

		return jdf


	def getProcessorData(self):
		return self.df
