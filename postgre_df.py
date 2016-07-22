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
from readprocessor_df import *
from readexcel_df import *

import psycopg2
import psycopg2.extras
import sys
import csv

import pprint


# Perform the select statement
def checkDB(conn, statement):

	cursor = conn.cursor() 
	cursor.execute(statement)

	rSet = cursor.fetchall()
	cursor.close()

	return rSet


# TODO - it's not called yet 
# Perform the insert statement
def insertDB(conn, statement):

	cursor = conn.cursor() 
	cursor.execute(statement)

	conn.commit()
	cursor.close()
	
	return True


def createTableFT(conn, ft_tableName, measure):

# 			"id integer NOT NULL DEFAULT nextval('ft_" + ft_tableName + "_id_seq'::regclass), " \

	createStatement = "CREATE TABLE public.ft_" + ft_tableName +" " \
		"( " \
 			"id integer NOT NULL DEFAULT nextval('ft_" + ft_tableName + "_id_seq'::regclass), " \
  			"data_id integer, "  \
  			"geographyloures_id integer, " + \
			measure + " integer, " \
  			"CONSTRAINT ft_" + ft_tableName + "_pkey PRIMARY KEY (id) " \
		")"
	print createStatement

	alterTableStatement = "ALTER TABLE public.ft_" + ft_tableName + " " \
    							"OWNER TO postgres;"

	createSeqStatement = "CREATE SEQUENCE public.ft_" + ft_tableName + "_id_seq " \
  							"INCREMENT 1 " \
  							"MINVALUE 1 " \
  							"MAXVALUE 9223372036854775807 " \
  							"START 1 " \
  							"CACHE 1; "

	alterTableSeqStatement = "ALTER TABLE public.ft_" + ft_tableName + "_id_seq " \
  							"OWNER TO postgres;"


	grantPostGresStatement = "GRANT ALL ON TABLE public.ft_" + ft_tableName + " TO postgres;"
	grantPublicStatement = "GRANT SELECT ON TABLE public.ft_" + ft_tableName + " TO public;"

	ckeckStatement = 'SELECT to_regclass(\'public.ft_' + ft_tableName + '\')'
	print ckeckStatement

	cursor = conn.cursor()
	cursor.execute(ckeckStatement)

	rSet = cursor.fetchall()
	print rSet[0][0]

	if str(rSet[0][0]) == 'None':
		cursor.close()

		cursor = conn.cursor()
		cursor.execute(createSeqStatement)
		conn.commit()
		cursor.execute(alterTableSeqStatement)
		conn.commit()

		cursor.execute(createStatement)
		conn.commit()

		cursor.execute(alterTableStatement)
		conn.commit()
		cursor.execute(grantPostGresStatement)
		conn.commit()
		cursor.execute(grantPublicStatement)
		conn.commit()

		cursor.close()





def validateHeaders(dataHdr, procHdr):

	pHdr = procHdr.columns.values.tolist()
	dHdr = dataHdr.columns.values.tolist()

	return  len(set(pHdr).intersection(dHdr)) == len(pHdr)


# Parsing process
def parseData(conn, ProcFileName, DataFileName):

	__DB_Test1 = False
	__XL_Test1 = True

	rdXL = readExcel(DataFileName, 'csv')

	if (__XL_Test1 == True):
		rdXL = readExcel('indicadores.xlsx', 'xls', 'indicadores',
						 ['date', 'DICOFRE', 'AREA_T_HA' ])
		return

	dfXL = rdXL.getExcelData()


	if (__DB_Test1 == True):
		createTableFT(conn, "populacao", "populacao")

		insertStatement = 'INSERT INTO ft_populacao (data_id, geographyloures_id, populacao) VALUES(1, 5, 300000)'
		insertDB(conn, insertStatement)

		return

	dfPKey = pd.DataFrame(index=dfXL.index, columns=dfXL.columns)

	rdProc = readProcessor(ProcFileName, 'csv')
#	rdProc = readProcessor('eenvplus_model.json', 'json', 0)

	dfProc = rdProc.getProcessorData()

#	print dfProc
#	print dfXL
#	print dfPKey

	# Validate the input data
	if (not validateHeaders(dfXL, dfProc)):
		print("Headers does not match !!")
		return

	for column in dfXL:
		if (dfProc.ix['type', column] == 'dm'):
#			print dfXL[column]

			filterDataStatement = 'ORDER BY data LIMIT 1'

			for ind, val in enumerate(dfXL[column]):

				if (dfProc.ix['table', column] == 'dm_date') :
					selectStatement = 'SELECT {0} FROM {1} WHERE {2} = \'{3}\' {4}'.format(dfProc.ix['pkey', column],
									dfProc.ix['table', column], dfProc.ix['value', column], val, filterDataStatement)
				else:
					selectStatement = 'SELECT {0} FROM {1} WHERE {2} = \'{3}\''.format(dfProc.ix['pkey', column],
													dfProc.ix['table', column], dfProc.ix['value', column], val)

#				print val
				print selectStatement
				tblId = checkDB(conn, selectStatement)

#				print (tblId)
				if (tblId == []):
					# TODO - insert into database not tested
					insertStatement = 'INSERT INTO {0} ({1}) VALUES(\'{2}\')'.format(dfProc.ix['table', column],
																				 dfProc.ix['value', column], val)
#					print(insertStatement)
#					insertDB(conn, insertStatement)

					maxStatement = 'SELECT max({0}) FROM {1}'.format(dfProc.ix['pkey', column],
																	 dfProc.ix['table', column])

					maxIdLst_dm = checkDB(conn, maxStatement)
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

				if (checkDB(conn, selectStatement) == []):
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
#					insertDB(conn, insertStatement)

					# It's not necessary - only for insertion check purpose
					maxStatement = 'SELECT max({0}) FROM {1}'.format(dfProc.ix['pkey', column],
																	 dfProc.ix['table', column])
#					print(maxStatement)
					maxIdLst_dm = checkDB(conn, maxStatement)
					dfPKey.at[ind,column] = maxIdLst_dm[0][0]


	print dfPKey




def main(csvProcFileName, csvDataFileName):

	conn_string = "host='localhost' dbname='django_cubes' user='django' password='django'"

	# print the connection string we will use to connect
	print ("Connecting to database\n	->%s" % (conn_string))
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this query to perform queries
	# note that in this example we pass a cursor_factory argument that will
	# dictionary cursor so COLUMNS will be returned as a dictionary so we
	# can access columns by their name instead of index.
	cursor = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
 
	# tell postgres to use more work memory
	work_mem = 4096
 
	# by passing a tuple as the 2nd argument to the execution function our
	# %s string variable will get replaced with the order of variables in
	# the list. In this case there is only 1 variable.
	# Note that in python you specify a tuple with one item in it by placing
	# a comma after the first variable and surrounding it in parentheses.
	cursor.execute('SET work_mem TO %s', (work_mem,))
 
	# Then we get the work memory we just set -> we know we only want the
	# first ROW so we call fetchone.
	# then we use bracket access to get the FIRST value.
	# Note that even though we've returned the columns by name we can still
	# access columns by numeric index as well - which is really nice.
	cursor.execute('SHOW work_mem')
 
	# Call fetchone - which will fetch the first row returned from the
	# database.
	memory = cursor.fetchone()
 
	# access the column by numeric index:
	# even though we enabled columns by name I'm showing you this to
	# show that you can still access columns by index and iterate over them.
	print ("Value: ", memory[0])

	# print the entire row 
	print ("Row:	", memory)


	parseData(conn, csvProcFileName, csvDataFileName)

if __name__ == "__main__":


	main('formato_ficheiro_processamento.txt',
		 'formato_ficheiro_carregamento.txt')