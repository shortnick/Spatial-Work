#! python3

"""
PTStoCSVtoSQL.py
Created by Nick Short

This script takes .pts files and copies them into a SQL database.


Workflow: finds .pts's in the same directory as the .py file, converts them to .csv's, makes tables with the same names in the given SQL database, copies the .csv into the SQL, then deletes the .csv files.
"""

import os
import shutil
import logging
import datetime
import csv

import psycopg2
from psycopg2 import sql


########################################
# User Input Variables

database = " " 	#"theSQLDataBase"  
user = " "		#"myUserName" in POSTgreSQL
pw = " "		#"mySuperSecretPassword" for the username 



##############################################################################
# Functions
def startCursor(dbase, user_name, pw):
	"""
	Uses psycopg2.connect(), returning a cursor object with an open POSTgreSQL connection. Use strings for inputs.
	Example: startCursor("theDataBase", "myUserName", "superSecretPassword") 
	"""
	loginString = "dbname="+dbase+" user="+user_name+" password="+pw
	logging.info("Database Request at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))
	return psycopg2.connect(loginString)

def make_table(table_name):
	"""
	Creates SQL table for Voxelizer output. Use string for input.
	"""
	
	theTable = sql.SQL("""CREATE TABLE {}
	    (
	      E double precision NOT NULL,
	      N double precision NOT NULL,
	      Z double precision NOT NULL,
	      R smallint NOT NULL,
	      Gee smallint NOT NULL,
	      B smallint NOT NULL,
	      Y smallint NOT NULL
	    );""").format(sql.Identifier(table_name))
	
	try:
		xCURSORx.execute(theTable)
		logging.info("SQL table created: "+table_name)
	except:
		logging.warning("Table creation failed.")
		logging.debug(theTable)

def copyCSVtoSQL(csvInput):
	cwd = os.getcwd()
	location = os.path.join(cwd,csvInput+"TEMP.csv")
	a = sql.SQL("""COPY "{}"(e, n, z, r, gee, b, y) """).format(sql.SQL(csvInput))
	b = sql.SQL("""FROM {} DELIMITER ' ' CSV HEADER;""").format(sql.Literal(location))
	logging.debug(sql.Composed([a,b]).as_string(conn))
	try:
		xCURSORx.execute(sql.Composed([a,b]))
		logging.debug(csvInput+" copied to SQL table.")
	except:
		logging.warning(csvInput+" NOT copied to SQL table.")

##############################################################################
# Main

workList = []

logging.basicConfig(filename='ptsIngester.log',level=logging.DEBUG)
logging.info("...\n...\nStarting ptsIngester at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))


#Make CSV copies of .pts files
for filename in os.listdir(os.getcwd()):
	if filename[-4:] ==".pts":
		try:
			workList.append(filename[:-4])
			newCSV = filename[:-4]+"TEMP.csv"
			shutil.copy2(filename, newCSV)
			logging.debug(filename[:-4]+" CSV copy done")
		except:
			logging.warning(filename+" not converted to CSV")
logging.info("Inputs prepared successfully")


#Make SQL tables, copy csv data into tables
conn= startCursor(database, user, pw)

with conn.cursor() as xCURSORx:

	for file in workList:
		make_table(file)

		copyCSVtoSQL(file)

	logging.info("Inputs converted to SQL tables")
	conn.commit()
	

#Clean up TEMP.csv's
for file in workList:
	if file+"TEMP.csv" in os.listdir():
		os.remove(file+"TEMP.csv")