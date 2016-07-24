#----------------------------------------------------------------------------------
#   JR - 2016-07-11
#
#
#
#   Processor Data File                                        Obs:
#
#   DATA,         GEO,                  VALOR                - Data Fields
#   dm,           dm,                   ft                   - Data Fields type
#   dm_date,      dm_geographyloures,   ft_corine            - Tables name
#   year,         freguesia_code,       artificial_surface   - Tables attribute for Check/Insert
#   id,           gid,                  id                   - dm Tables primary key attributes
#   data_id,      geographyloures_id,   none                 - ft Table foreign keys attributes (dummy - none)
#
#----------------------------------------------------------------------------------

from collections import OrderedDict
import sys
import csv

import pprint

from postgre_df import *
from readprocessor_df import *
from readexcel_df import *



def validateHeaders(dataHdr, procHdr):

	pHdr = procHdr.columns.values.tolist()
	dHdr = dataHdr.columns.values.tolist()

	return  len(set(pHdr).intersection(dHdr)) == len(pHdr)


# Parsing process
def parseData(ProcFileName, DataFileName):

	__DB_Test1 = True
	__XL_Test1 = True

	__def_ft_name = 'populacao'
	__def_ft_attr = 'populacao integer'
	__def_sheet_name = 'indicadores'

	pgDB = postgresDB()

	if (__XL_Test1 == True):
		# From xls file (non ine)
		DataFileName = 'indicadores.xlsx'
		ProcFileName = 'eenvplus_model.json'
		rdXL = readExcel(DataFileName, 'xls', __def_sheet_name,
						 ['date', 'DICOFRE', 'TOT_POP91' ], ['year', 'freguesia_code', __def_ft_name])
		rdProc = readProcessor(ProcFileName, 'json', __def_ft_name)
	else:
		# Initial csv txt files version
		rdXL = readExcel(DataFileName, 'csv')
		rdProc = readProcessor(ProcFileName, 'csv')

	dfXL = rdXL.getExcelData()

	dfPKey = pd.DataFrame(index=dfXL.index, columns=dfXL.columns)

	dfProc = rdProc.getProcessorData()


#	print dfProc
#	print dfXL
#	print dfPKey
	if __DB_Test1 == True:
		pgDB.createTableFT(__def_ft_name, dfProc.ix['fkey', 0],
		                                  dfProc.ix['fkey', 1], __def_ft_attr)
		insertStatement = 'INSERT INTO ft_populacao (' + dfProc.ix['fkey', 0] + ', ' + \
						                        	     dfProc.ix["fkey", 1] + ', populacao) VALUES(1, 5, 300000)'
#		pgDB.insertDB(insertStatement)



	if not validateHeaders(dfXL, dfProc):
		print("Headers does not match !!")
		return

	for column in dfXL:
		if (dfProc.ix['type', column] == 'dm'):
#			print dfXL[column]

			for ind, val in enumerate(dfXL[column]):

				if (dfProc.ix['table', column] == 'dm_date') :
					filterDataStatement = 'ORDER BY data LIMIT 1'

					# TODO - check if date has 4, 6 or 8 chars and insert the 1st day in data + month_...
					selectStatement = 'SELECT {0} FROM {1} WHERE {2} = \'{3}\' {4}'.format(dfProc.ix['pkey', column],
									dfProc.ix['table', column], dfProc.ix['value', column], val, filterDataStatement)
				else:
					selectStatement = 'SELECT {0} FROM {1} WHERE {2} = \'{3}\''.format(dfProc.ix['pkey', column],
													dfProc.ix['table', column], dfProc.ix['value', column], val)

#				print val
				print selectStatement
				tblId = pgDB.checkDB(selectStatement)

#				print (tblId)
				if (tblId == []):
					# TODO - insert into database not tested
					insertStatement = 'INSERT INTO {0} ({1}) VALUES(\'{2}\')'.format(dfProc.ix['table', column],
																				 dfProc.ix['value', column], val)
#					print(insertStatement)
#					pgDB.insertDB(insertStatement)

					maxStatement = 'SELECT max({0}) FROM {1}'.format(dfProc.ix['pkey', column],
																	 dfProc.ix['table', column])

					maxIdLst_dm = pgDB.checkDB(maxStatement)
					maxId_dm = maxIdLst_dm[0][0]

					dfPKey.at[ind,column] =  maxId_dm
				else:
					dfPKey.at[ind,column] = tblId[0][0]

	# set the dm fields to be used to construct the where expressions
	dm_headers = []     # dm headers names
	ft_joinFields = []  # ft join fields names

	for column in dfXL:
		if (dfProc.ix['type', column] == 'dm'):
				dm_headers.append(column)
				ft_joinFields.append(dfProc.ix['fkey', column])

#	print (dm_headers)
#	print (ft_joinFields)

	# Check if the set of dm id's already exists in ft table
	# if not insert into ft table
	for column in dfXL:
		if (dfProc.ix['type', column] == 'ft'):
			for ind, val in enumerate(dfXL[column]):

				strWhere = ""
				# Construct the sql filter string with the dm fields to check inside the ft table
				for dm_field in dm_headers:
					if (len(strWhere) > 0):
						strWhere = strWhere + ' AND '

					strWhere = strWhere + dfProc.ix['fkey', dm_field] + ' = ' + str(dfPKey.ix[ind, dm_field])

				selectStatement = 'SELECT {0} FROM {1} WHERE {2}'.format(dfProc.ix['pkey', column],
																		 dfProc.ix['table', column], strWhere)

				if (pgDB.checkDB(selectStatement) == []):
					insertFields = ''
					insertValues = ''

					# Construct the sql filter string with the dm fields to insert into the ft table
					for ff, field in enumerate(dm_headers):
						if (len(insertFields) > 0):
							insertFields = insertFields + ', '
							insertValues = insertValues + ', '
						insertFields = insertFields + str(ft_joinFields[ff])
						insertValues = insertValues + str(dfPKey.ix[ind, field])

					# TODO - insert into database not tested
					insertStatement = 'INSERT INTO {0} ({1}, {2}) VALUES({3}, {4})'.format(dfProc.ix['table', column],
						                     dfProc.ix['value', column], insertFields, val, insertValues)
					print(insertStatement)
					pgDB.insertDB(insertStatement)

					# It's not necessary - only for insertion check purpose
					maxStatement = 'SELECT max({0}) FROM {1}'.format(dfProc.ix['pkey', column],
																	 dfProc.ix['table', column])
#					print(maxStatement)
					maxIdLst_dm = pgDB.checkDB(maxStatement)
					dfPKey.at[ind,column] = maxIdLst_dm[0][0]


	print dfPKey



if __name__ == "__main__":



    parseData('formato_ficheiro_processamento.txt', 'formato_ficheiro_carregamento.txt')
