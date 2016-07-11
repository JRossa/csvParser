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
#   id,           gid,                  dm_att               - dm Tables primary key attributes (dummy - field in ft column)
#   data_id,      geographyloures_id,   id                   - ft Table foreign keys attributes (dummy - ft Table primary key)
#
#----------------------------------------------------------------------------------


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


# Read csv fileName data, for header and data
def readCSVFileHeader(fileName):

	column = {}

	with open(fileName, 'r') as f:
		has_header = csv.Sniffer().has_header(f.read(1024))
#		print (has_header)
		f.seek(0)  # rewind
		reader = csv.reader(f)

		if (has_header):
			headers = next(reader)

			column = {h:[] for h in headers}
			for row in reader:
				for h, v in zip(headers, row):
					column[h].append(v)

	return column


# Read csv fileName data, for header and processor data (when there is no separate header, sniffer has_header fails)
def readCSVFile(fileName):

	column = {}

	with open(fileName, 'r') as f:
		f.seek(0)  # rewind
		reader = csv.reader(f)

		headers = next(reader)

		column = {h:[] for h in headers}
		for row in reader:
			for h, v in zip(headers, row):
				column[h].append(v)

	return column


# Check for the consistency of headers fields 
def validateHeaders(dataHdr, procHdr):

	for hh, hdr in enumerate(procHdr):
		print(hdr)
		try:
			print(procHdr[hdr] == [])
		except:
			print("False")
			return False

	return True


# Parsing process
def parseData(conn, csvProcFileName, csvDataFileName):

	csvData = readCSVFileHeader(csvDataFileName)

	csvProc = readCSVFile(csvProcFileName)

#	pprint (csvProc)

	headers = []

	# Create the headers os column data fields to store the primary keys of dm and ft tables
	for v, val in enumerate(csvProc):
		headers.append(val)

	column = {h:[] for h in headers}

	# Validate the input data
	if (not validateHeaders(csvData, column)):
		print("Headers does not match !!")
		return

	# For each dm field insert into dm table if it does not exists
	# and store the id value 
	for hh, hdr in enumerate(column):
		if (csvProc[hdr][0] == 'dm'):

			for vv, val in enumerate(csvData[hdr]):
				selectStatement = 'SELECT {0} FROM {1} WHERE {2} = \'{3}\''.format(csvProc[hdr][3], 
																					csvProc[hdr][1], csvProc[hdr][2], val)
#				print(selectStatement)
				tblId = checkDB(conn, selectStatement)

#				print (tblId)
				if (tblId == []):
					# TODO - insert into database not tested
					insertStatement = 'INSERT INTO {0} ({1}) VALUES({2})'.format(csvProc[hdr][1], csvProc[hdr][2], val)
					print(insertStatement)
#					insertDB(conn, insertStatement)

					maxStatement = 'SELECT max({0}) FROM {1}'.format(csvProc[hdr][3], csvProc[hdr][1])

					nextIdLst_dm = checkDB(conn, maxStatement)
					nextId_dm = nextIdLst_dm[0][0]
					column[hdr].append(nextId_dm)
				else:
					column[hdr].append(tblId[0][0])

	# set the dm fields to be used to construct the where expressions				
	dm_headers = []     # dm headers names
	ft_joinFields = []  # ft join fields names

	for hh, hdr in enumerate(column):
		if (csvProc[hdr][0] == 'dm'):
				dm_headers.append(headers[hh])
				ft_joinFields.append(csvProc[hdr][4])

#	print (dm_headers)
#	print (ft_joinFields)
#	print (column)

	# Check if the set of dm id's already exists in ft table
	# if not insert into ft table
	for hh, hdr in enumerate(column):
		if (csvProc[hdr][0] == 'ft'):
			for vv, val in enumerate(csvData[hdr]):
				# print (vv, "   ", csvProc[hdr][0])
				# print (csvProc[hdr][4])
				# print (csvProc[hdr][1])
				# print (csvProc[hdr][2])

				strWhere = ""
				# Construct the sql filter string with the dm fields to check inside the ft table
				for ff, dm_field in enumerate(dm_headers):
					if (len(strWhere) > 0):
						strWhere = strWhere + ' AND '
					# WARNING - same times I get empty values from dm_field 
					#           UnboundLocalError: local variable 'dm_field' referenced before assignment
					try:	
						strWhere = strWhere + csvProc[dm_field][4] + ' = ' + str(column[dm_field][vv])
					except:
						print("hh = ", hh, "vv = ", vv, " ff = ", ff, " Field = ", dm_field, "  Str = ", strWhere)
						print("empty values from dm_field ")
						return

#				print (csvProc[hdr][4])
#				print (csvProc[hdr][1])
				selectStatement = 'SELECT {0} FROM {1} WHERE {2}'.format(csvProc[hdr][4], csvProc[hdr][1], strWhere)

				if (checkDB(conn, selectStatement) == []):
					insertFields = ''
					insertValues = ''

					# Construct the sql filter string with the dm fields to insert into the ft table
					for ff, field in enumerate(dm_headers):
						if (len(insertFields) > 0):
							insertFields = insertFields + ', '
							insertValues = insertValues + ', '
						insertFields = insertFields + str(ft_joinFields[ff])
						insertValues = insertValues + str(column[field][vv])

					# TODO - insert into database not tested
					insertStatement = 'INSERT INTO {0} ({1}, {2}) VALUES({3}, {4})'.format(csvProc[hdr][1], csvProc[hdr][2],
											 insertFields, val, insertValues)
					print(insertStatement)
#					insertDB(conn, insertStatement)

					# It's not necessary - only for insertion check purpose
					maxStatement = 'SELECT max({0}) FROM {1}'.format(csvProc[hdr][4], csvProc[hdr][1])
#					print(maxStatement)
					nextIdLst_dm = checkDB(conn, maxStatement)
					nextId_dm = nextIdLst_dm[0][0]
					print (nextId_dm)

#	print (column)



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