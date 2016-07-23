
import psycopg2
import psycopg2.extras

class postgresDB:

	def __init__(self):

		conn_string = "host='localhost' dbname='django_cubes' user='django' password='django'"

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

		cursor = self.conn.cursor()
		cursor.execute(ckeckStatement)

		rSet = cursor.fetchall()
		print rSet[0][0]

		if str(rSet[0][0]) == 'None':
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
		createStatement = "CREATE TABLE public.ft_" + ft_tableName +" " \
			"( " \
				"id SERIAL, " + \
				fKey1 + " integer, " + \
				fKey2 + " integer, " + \
				measure + ", " \
				"CONSTRAINT ft_" + ft_tableName + "_pkey PRIMARY KEY (id) " \
			")"
		print createStatement

		alterTableStatement = "ALTER TABLE public.ft_" + ft_tableName + " " \
									"OWNER TO postgres;"

		grantPostGresStatement = "GRANT ALL ON TABLE public.ft_" + ft_tableName + " TO postgres;"
		grantPublicStatement = "GRANT SELECT ON TABLE public.ft_" + ft_tableName + " TO public;"

		ckeckStatement = 'SELECT to_regclass(\'public.ft_' + ft_tableName + '\')'
		print ckeckStatement

		cursor = self.conn.cursor()
		cursor.execute(ckeckStatement)

		rSet = cursor.fetchall()
		print rSet[0][0]

		if str(rSet[0][0]) == 'None':
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





