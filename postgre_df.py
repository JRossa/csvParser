#----------------------------------------------------------------------------------
#   JR - 2016-07-29
#
# 1. Create directory and file
# ./definitions/postgresDB.conf
#
# 2. Set the config params
#  host=localhost
#  dbname=django_cubes
#  username=django
#  password=django
#
#
#----------------------------------------------------------------------------------

import psycopg2
import psycopg2.extras

import sys
sys.path.insert(0, 'excelreader/')

from definitions import *

class postgresDB:

	def __init__(self):

		conn_string = self.getConnection("./definitions/postgresDB.conf")

		# print the connection string we will use to connect
		print ("Connecting to database\n	->%s" % (conn_string))

		self.conn = None

		# get a connection, if a connect cannot be made an exception will be raised here
		self.conn = psycopg2.connect(conn_string)


		# conn.cursor will return a cursor object, you can use this query to perform queries
		# note that in this example we pass a cursor_factory argument that will
		# dictionary cursor so COLUMNS will be returned as a dictionary so we
		# can access columns by their name instead of index.
		cursor = self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

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


	# Perform the select statement
	def checkDB(self, statement):

		if self.conn == None:
			return []

		cursor = self.conn.cursor()
		cursor.execute(statement)

		rSet = cursor.fetchall()
		cursor.close()

		return rSet


	# read the config file
	def getConnection(self, definitions_file):

		try:
			definitions = Definitions(definitions_file)
			host = definitions.config['host']
			dbname= definitions.config['dbname']
			user = definitions.config['username']
			password = definitions.config['password']

			print definitions_file
			conn_string = "host=" + host + " dbname=" + dbname + " user=" + user + " password=" + password
		except:
			conn_string = "host='localhost' dbname='django_cubes' user='django' password='django'"

		return conn_string


	# Perform the insert statement
	def insertDB(self, statement):

		if self.conn == None:
			return False

		cursor = self.conn.cursor()
		cursor.execute(statement)

		self.conn.commit()
		cursor.close()

		return True


	def createTableAndSeqFT(self, ft_tableName, measure):

		if self.conn == None:
			return

		createStatement = 'CREATE TABLE public.' + ft_tableName + ' ' \
			'( ' \
				'id integer NOT NULL DEFAULT nextval(\'' + ft_tableName + '_id_seq\'::regclass), ' \
				'data_id integer, '  \
				'geographyloures_id integer, ' + \
				measure + ' integer, ' \
				'CONSTRAINT ' + ft_tableName + '_pkey PRIMARY KEY (id) ' \
			')'
		print createStatement

		alterTableStatement = 'ALTER TABLE public.' + ft_tableName + ' ' \
									'OWNER TO postgres;'

		createSeqStatement = 'CREATE SEQUENCE public.' + ft_tableName + '_id_seq ' \
								'INCREMENT 1 ' \
								'MINVALUE 1 ' \
								'MAXVALUE 9223372036854775807 ' \
								'START 1 ' \
								'CACHE 1; '

		alterTableSeqStatement = 'ALTER TABLE public.' + ft_tableName + '_id_seq ' \
								'OWNER TO postgres;'


		grantPostGresStatement = 'GRANT ALL ON TABLE public.' + ft_tableName + ' TO postgres;'
		grantPublicStatement = 'GRANT SELECT ON TABLE public.' + ft_tableName + ' TO public;'

#		ckeckStatement = 'SELECT to_regclass(\'public.ft_' + ft_tableName + '\')'
		ckeckStatement = 'SELECT EXISTS (SELECT 1 FROM   information_schema.tables ' \
   							'WHERE table_schema = \'public\' AND ' \
   							'table_name = \'' + ft_tableName + '\')'

		print ckeckStatement

		cursor = self.conn.cursor()
		cursor.execute(ckeckStatement)

		rSet = cursor.fetchall()
		print rSet[0][0]

#		if str(rSet[0][0]) == 'None':
		if rSet[0][0] == False:
			cursor.close()

			cursor = self.conn.cursor()
			cursor.execute(createSeqStatement)
			self.conn.commit()
			cursor.execute(alterTableSeqStatement)
			self.conn.commit()

			cursor.execute(createStatement)
			self.conn.commit()

			cursor.execute(alterTableStatement)
			self.conn.commit()
			cursor.execute(grantPostGresStatement)
			self.conn.commit()
			cursor.execute(grantPublicStatement)
			self.conn.commit()

			cursor.close()


	def createTableFT(self, ft_tableName, fKey1, fKey2, measure):

		if self.conn == None:
			return

		# https://www.postgresql.org/docs/9.2/static/datatype-numeric.html#DATATYPE-SERIAL
		createStatement = 'CREATE TABLE public.' + ft_tableName + ' ' \
			'( ' \
				'id SERIAL, ' + \
				fKey1 + ' integer, ' + \
				fKey2 + ' integer, ' + \
				measure + ', ' \
				'CONSTRAINT ' + ft_tableName + '_pkey PRIMARY KEY (id) ' \
			')'
		print createStatement

		alterTableStatement = 'ALTER TABLE public.' + ft_tableName + ' ' \
									'OWNER TO postgres;'

		grantPostGresStatement = 'GRANT ALL ON TABLE public.' + ft_tableName + ' TO postgres;'
		grantPublicStatement = 'GRANT SELECT ON TABLE public.' + ft_tableName + ' TO public;'

		ckeckStatement = 'SELECT to_regclass(\'public.' + ft_tableName + '\')'
		ckeckStatement = 'SELECT EXISTS (SELECT 1 FROM   information_schema.tables ' \
   							'WHERE table_schema = \'public\' AND ' \
   							'table_name = \'' + ft_tableName + '\')'
		print ckeckStatement

		cursor = self.conn.cursor()
		cursor.execute(ckeckStatement)

		rSet = cursor.fetchall()
		print rSet[0][0]


#		if str(rSet[0][0]) == 'None':
		if rSet[0][0] == False:
			cursor.close()

			cursor = self.conn.cursor()

			cursor.execute(createStatement)
			self.conn.commit()

			cursor.execute(alterTableStatement)
			self.conn.commit()
			cursor.execute(grantPostGresStatement)
			self.conn.commit()
			cursor.execute(grantPublicStatement)
			self.conn.commit()

			cursor.close()





