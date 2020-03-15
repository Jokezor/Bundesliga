# This will hold the database functions

import mysql.connector # required

# Connect to the database. Inputs are strings.
def connect_to_db(host, database, user, password):
	mydb = mysql.connector.connect(
	    host=host,
	    database=database,
	    user=user,
	    passwd=password,
	    auth_plugin='mysql_native_password'
	)

	return mydb

def get_table_queries(Create=False, Insert=False, Drop=False):

	if Create:
		return ["CREATE TABLE Matches (\
				  Match_ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\
				  Division CHAR(2),\
				  Date_Played DATE,\
				  Home_Team VARCHAR(60),\
				  Away_Team VARCHAR(60),\
				  FTHG INT,\
				  FTAG INT,\
				  FTR CHAR(1),\
				  HTHG INT DEFAULT 0,\
				  HTAG INT DEFAULT 0,\
				  HTR CHAR(1) DEFAULT 'D',\
				  Season VARCHAR(15),\
				  PRIMARY KEY (Match_ID)\
				)",\
			
			  "CREATE TABLE Teams (\
				  Team_ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\
				  Name VARCHAR(60),\
				  Wins INT UNSIGNED,\
				  Home_Wins INT UNSIGNED,\
				  Away_Wins INT UNSIGNED,\
				  Goals_scored INT UNSIGNED,\
				  PRIMARY KEY (Team_ID)\
				)",\

			  "CREATE TABLE Scores (\
				  Score_ID INT UNSIGNED NOT NULL AUTO_INCREMENT,\
				  Team_ID INT UNSIGNED,\
				  Total_score INT UNSIGNED,\
				  PRIMARY KEY (Score_ID)\
				)"]

	elif Drop:
		return ["DROP TABLE Matches",\
				"DROP TABLE Teams",\
				"DROP TABLE Scores"]

	elif Insert:
		return {'Matches': "INSERT INTO Matches (Division, Date_Played, Home_Team, Away_Team, FTHG, FTAG, FTR, HTHG, HTAG, HTR, Season) VALUES ",\
				'Teams': "INSERT INTO Teams (Name, Wins, Home_Wins, Away_Wins, Goals_scored) VALUES ",\
				'Scores': "INSERT INTO Scores (Team_ID, Total_score) VALUES "}

# Connects to the database and creates tables. Inputs are strings, Boolean to create new tables or not.
def create_tables(host, database, user, password, drop_old = False):

	mydb = connect_to_db(host, database, user, password)

	# Drops tables
	if drop_old:
		for drop_query in get_table_queries(Drop=True):
			try:
				mydb.cursor().execute(drop_query)
			except mysql.connector.Error as err:
				# No existing table
				if err.errno == 1051:
					pass
				else:
					raise
	# Create tables
	for create_query in get_table_queries(Create=True):

		try:
			mydb.cursor().execute(create_query)
		except mysql.connector.Error as err:
			# Tables already exists
			if err.errno == 1050:
				pass
			else:
				raise

# host, database, user, password, table are strings. data is list.
def insert_data(host, database, user, password, table, data):

	mydb = connect_to_db(host, database, user, password)

	if table == 'Matches':
		mydb.cursor().execute(get_table_queries(Insert=True)['Matches'] + "%r" %(tuple(data),))


#

