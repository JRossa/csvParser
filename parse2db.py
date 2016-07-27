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
import argparse

from postgre_df import *
from readprocessor_df import *
from readexcel_df import *


__insert_DM = 1
__insert_FT = 2
__create_FT = 4

def validateHeaders(dataHdr, procHdr):

	pHdr = procHdr.columns.values.tolist()
	dHdr = dataHdr.columns.values.tolist()

	return  len(set(pHdr).intersection(dHdr)) == len(pHdr)


# Parsing process
def parseData(pgDB, dfProc, dfXL, dbOptions):

	dfPKey = pd.DataFrame(index=dfXL.index, columns=dfXL.columns)

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
					print(insertStatement)
					if (dbOptions & __insert_DM) == __insert_DM:
						pgDB.insertDB(insertStatement)

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
					if (dbOptions & __insert_FT) == __insert_FT:
						pgDB.insertDB(insertStatement)

					# It's not necessary - only for insertion check purpose
					maxStatement = 'SELECT max({0}) FROM {1}'.format(dfProc.ix['pkey', column],
																	 dfProc.ix['table', column])
#					print(maxStatement)
					maxIdLst_dm = pgDB.checkDB(maxStatement)
					dfPKey.at[ind,column] = maxIdLst_dm[0][0]


	print dfPKey


def test1(procFileName, procType, dataFileName,  dataType):

	__DB_Options =  __create_FT | __insert_FT

	# Initial csv txt files version
	rdXL = readExcel(dataFileName, dataType)
	rdProc = readProcessor(procFileName,  procType)

	pgDB = postgresDB()
	dfProc = rdProc.getProcessorData()
	dfXL = rdXL.getExcelData()

	if (__DB_Options & __create_FT) == __create_FT:
		__def_ft_attr = dfProc.columns[-1:][0] + ' integer'

		pgDB.createTableFT(dfProc.ix['table', 2], dfProc.ix['fkey', 0],
		                                  dfProc.ix['fkey', 1], __def_ft_attr)

	print dfProc

	print dfXL
#	print dfPKey

	parseData(pgDB, dfProc, dfXL, __DB_Options)


def test2(procFileName, procType, dataFileName, dataType, sheetName, columnsSearch, ftName):

	__DB_Options =  __create_FT | __insert_FT

	rdProc = readProcessor(procFileName, procType, ftName)
	dfProc = rdProc.getProcessorData()

	rdXL = readExcel(dataFileName, dataType, sheetName,
					 columnsSearch, dfProc.columns.tolist())

	pgDB = postgresDB()
	dfXL = rdXL.getExcelData()

	if (__DB_Options & __create_FT) == __create_FT:
		__def_ft_attr = dfProc.columns[-1:][0] + ' integer'

		pgDB.createTableFT(dfProc.ix['table', 2], dfProc.ix['fkey', 0],
		                                  dfProc.ix['fkey', 1], __def_ft_attr)
		insertStatement = 'INSERT INTO ft_populacao (' + dfProc.ix['fkey', 0] + ', ' + \
						                        	     dfProc.ix["fkey", 1] + ', populacao) VALUES(1, 5, 300000)'
#		pgDB.insertDB(insertStatement)

	print dfProc

	print dfXL
#	print dfPKey

	parseData(pgDB, dfProc, dfXL, __DB_Options)


def test3(procFileName, procType, procFtName, dataFileName, dataType, sheetName, columnsSearch, configParam):

	__DB_Options =  __create_FT | __insert_FT

	__fileName = 'excelreader/' + dataFileName

	rdProc = readProcessor(procFileName, procType, procFtName)
	dfProc = rdProc.getProcessorData()

	__dataFileName = 'excelreader/' + dataFileName

	rdXL = readExcel(__dataFileName, dataType, sheetName,
					 columnsSearch, dfProc.columns.tolist(), configParam)

	pgDB = postgresDB()
	dfXL = rdXL.getExcelData()

	if (__DB_Options & __create_FT) == __create_FT:
		__def_ft_attr = dfProc.columns[-1:][0] + ' integer'

		pgDB.createTableFT(dfProc.ix['table', 2], dfProc.ix['fkey', 0],
		                                  dfProc.ix['fkey', 1], __def_ft_attr)
		insertStatement = 'INSERT INTO ft_populacao (' + dfProc.ix['fkey', 0] + ', ' + \
						                        	     dfProc.ix["fkey", 1] + ', populacao) VALUES(1, 5, 300000)'
#		pgDB.insertDB(insertStatement)

	print dfProc

	print dfXL
#	print dfPKey


	parseData(pgDB, dfProc, dfXL, __DB_Options)



def test7(dataFileName, dataType, sheetName, configParam):

	__fileName = 'excelreader/' + dataFileName

	rdXL = readExcel(__fileName, dataType)
	dfXL = rdXL.getExcelMetaData(__fileName, sheetName, configParam)

	print json.dumps(dfXL)


def test8(dataFileName,  dataType, sheetName=None):

	# Initial csv txt files version
	rdXL = readExcel(dataFileName, dataType)
	dfXL = rdXL.getExcelData()

	print json.dumps(list(set(dfXL.columns)))


if __name__ == "__main__":

	__def_Test = 5
	__def_ParseMain = True

	if __def_ParseMain == True:
		parser = argparse.ArgumentParser(description='Process application tests.')
		parser.add_argument('test', metavar='Test Number', type=int, # nargs='+'
					   help=' - an integer for select the test')
		args = parser.parse_args()

		# only one arg -> args.test
		# several args (nargs='+') -> args.test[..n]
		if args.test > 0 and args.test < 20:
			print args.test
			__def_Test = args.test

	print "Test n. " + str(__def_Test)

	if __def_Test == 1:
		# Initial csv txt files (date, geo, data) version
		test1('formato_ficheiro_processamento.txt', 'csv', 'formato_ficheiro_carregamento.txt', 'csv')
	elif  __def_Test == 2:
		# excel file (non ine) with (date, geo, [select measure]) version
		test2('eenvplus_model.json', 'json', 'indicadores.xlsx', 'xls', 'indicadores', ['date', 'DICOFRE', 'TOT_POP91' ], 'populacao')
	elif  __def_Test == 3:
		# excel file (ine) with (date, geo, [select measure]) version
		test3('eenvplus_model.json', 'json', 'cos',
			      'superficie_uso_industrial_solo.xls', 'xls_ine', 'Quadro',
			            {'date': '2013', 'geo' : 'Localizacao', 'data' : 'area'},
			            {'delete_rows': [0,1,2,3,4,6], 'filldown':[0], 'fillright':[0], 'multi-index': 1})
	elif  __def_Test == 4:
		# excel file (ine) with (date, geo, [select measure]) version
		test3('eenvplus2_model.json', 'json', 'vinho_n_certificado',
			       'producao_vinicola_declarada_vinho.xls', 'xls_ine', 'Quadro',
		      {'date' : '2014', 'geo' : 'Local de vinificacao (NUTS - 2013)', 'data' : '5: Vinho sem certificacao'},
			            {'delete_rows': [0,1,2,3,4,6,8,10], 'filldown':[0], 'fillright':[0, 1], 'multi-index': 2})
	elif  __def_Test == 5:
		# excel file (ine) with (date, geo, [select measure]) version
		test3('eenvplus2_model.json', 'json', 'vinho_n_certificado',
			       'producao_vinicola_declarada_vinho.xls', 'xls_ine', 'Quadro',
		      {'date' : '2015', 'geo' : 'Local de vinificacao (NUTS - 2013)', 'data' : '5: Vinho sem certificacao'},
			            {'delete_rows': [0,1,2,3,4,6,8,10], 'filldown':[0], 'fillright':[0, 1], 'multi-index': 2})
	elif  __def_Test == 7:
		# excel file reads file metadata
		test7('superficie_uso_industrial_solo.xls', '', 'Quadro',
			            {'delete_rows': [0,1,2,3,4,6], 'filldown':[0], 'fillright':[0], 'multi-index': 1})
	elif  __def_Test == 8:
		# excel file reads file metadata
		test7('producao_vinicola_declarada_vinho.xls', '', 'Quadro',
			            {'delete_rows': [0,1,2,3,4,6], 'filldown':[0], 'fillright':[0], 'multi-index': 1})
	elif  __def_Test == 9:
		# excel file reads file metadata
		test8('formato_ficheiro_processamento.txt', 'csv')
	elif  __def_Test == 10:
		# excel file reads file metadata
		test8('indicadores.xlsx', 'xls', 'indicadores')
	else:
		print "no test option"