#! python3	

"""
Point Voxelizer
Version 0.2
Created by Nick Short

This program uses PostgreSQL and the psycopg2 library to break a point cloud into an arbitrary number of boxes and select the lowest elevation point within each of those boxes, returning a much lower density point cloud.


"""

##############################################################################
import logging
import datetime
import csv

import psycopg2
from psycopg2 import sql


########################################
# User Input Variables
binCount = 4
binMax = abs(binCount)
overwriteFiles=False
inputSQLtable='ptstiny'
outputFilename='ptsTinyVoxel'
database = 'quest'
user = 'basic'
pw = 'postgrescorvus'
writeToSQL=True
writeToCSV=True

########################################
# Selection & Calculation Variables (placeholders)

maxN, minN, countN, gapN, stepN= [-9, -9, -9, -9, -9]
maxE, minE, countE, gapE, stepE= [-9, -9, -9, -9, -9]
maxZ, minZ, countZ, gapZ, stepZ= [-9, -9, -9, -9, -9]
nbinLow, nbinHi = [-9,-9]
ebinLow, ebinHi = [-9,-9]
zbinLow, zbinHi = [-9,-9]
nbinList= [] 
ebinList= [] 
zbinList= []
outputBin = []


##############################################################################
# Functions

########################################
# Database/SQL Functions
def startCursor(dbase, user_name, pw):
	"""
	Uses psycopg2.connect(), returning a cursor object with an open POSTgreSQL connection. Use strings for inputs.
	Example: startCursor("theDataBase", "myUserName", "superSecretPassword") 
	"""
	loginString = "dbname="+dbase+" user="+user_name+" password="+pw
	logging.info("Database Request at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))
	return psycopg2.connect(loginString)

def fetchParams(table_name):
	"""
	Cursor retrieves important parameters of E, N, Z coordinate columns from SQL table. Use string for input.
	"""
	global maxN, minN, countN, gapN
	global maxE, minE, countE, gapE
	global maxZ, minZ, countZ, gapZ
	
	query = sql.SQL("""SELECT max(n), min(n), count(n), (max(n)-min(n)), max(e), min(e), count(e), (max(e)-min(e)), max(z), min(z), count(z), (max(z)-min(z)) FROM {};""").format(sql.Identifier(table_name))
	
	try:
		xCURSORx.execute(query)
	except:
		logging.warning(query)
	maxN, minN, countN, gapN, maxE, minE, countE, gapE, maxZ, minZ, countZ, gapZ = (xCURSORx.fetchall()[0])
	logging.debug(" \n maxN = %s, minN = %s, countN = %s,  \n maxE = %s, minE = %s, countE = %s,  \n maxZ = %s, minZ = %s, countZ = %s"%(maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ))
	if countN != countZ or countZ != countE:
		logging.warning("Record counts return mismatched numbers.")

def sql_table_exists(table_name):
	"""
	Verifies that user's requested table is a public table in the SQL database. Use string for input.
	"""
	query= sql.SQL("""SELECT EXISTS 
		(
		SELECT 1
		FROM information_schema.tables 
		WHERE table_schema = 'public'
		AND table_name = {}
		);""").format(sql.Literal(table_name))
	xCURSORx.execute(query)
	extant = xCURSORx.fetchall()[0][0]
	logging.debug("SQL table "+table_name+" exists = "+str(extant))
	return extant

def make_table(table_name):
	"""
	Creates SQL table for Voxelizer output. Use string for input.
	"""
	global outputFilename 
	
	theTable = sql.SQL("""CREATE TABLE {}
	    (
	      E double precision NOT NULL,
	      N double precision NOT NULL,
	      Z double precision NOT NULL,
	      RGBY character varying(50)
	    );""").format(sql.Identifier(table_name))
	
	try:
		xCURSORx.execute(theTable)
		logging.info("SQL table created: "+table_name)
	except:
		logging.warning("Table creation failed.")
		logging.debug(theTable)

def write_SQL_output(table_name):
	"""
	Uses contents of outputBin (aka the selected lowest elevation points), writes each tuple into a SQL table row. Use string for input.
	"""
	global outputBin
	logList=[]
	logging.debug(outputBin)
	for tupleOut in outputBin:
		east, north, up, color = tupleOut
		a = sql.SQL("INSERT INTO {tbl} (E, N, Z, RGBY) VALUES (%(east)s, %(north)s, %(up)s, %(color)s);").format(tbl=sql.Identifier(table_name))
		b= ({'east':east, 'north':north, 'up':up, 'color':color})
		#Use this to see SQL statement for each point: logging.debug("SQL: "+str(a)+str(b))
		try:
			xCURSORx.execute(a,b)
			logList.append(tupleOut)
		except:
			logging.warning("Tuple "+tupleOut+" not written to SQL.")
	logging.debug("Points written to SQL-"+table_name+".")
	logging.debug(logList)

def write_csv_out(outList, filename):
	"""
	Writes contents of outputBin to a csv, one tuple per row, with no header.
	Use list, string as inputs.
	"""
	logList=[]
	fileOut = filename+'.csv'
	quotechar="'"
	with open(fileOut, 'w', newline='') as outputCSV:
		pointwriter = csv.writer(outputCSV, delimiter= ',', quotechar=quotechar, quoting=csv.QUOTE_MINIMAL)

		for lineOut in outList:
			try:
				pointwriter.writerow(lineOut)
				logList.append(lineOut)
			except:
				logging.warning("CSV line "+ lineOut+" not written.")
	logging.debug("Points written to CSV "+filename+".")
	logging.debug(logList)

def output_handler():
	"""
	Function takes desired filename, overwrite preference, and which file formats to export. 
	Example: output_handler('myFilename', False, True, True)
	Unless overwrite==True, timestamp (HMS) is appended to fileName before write functions are called.
	"""
	global outputFilename
	
	
	if overwriteFiles==False:
		#This writes new csv/table with HourMinSec appended to outputFilename. 
		outputFilename = outputFilename+datetime.datetime.now().strftime("%H%M%S")
		if writeToCSV==True:
			write_csv_out(outputBin, outputFilename)
		else: 
			print('')
		if writeToSQL==True:
			make_table(outputFilename)
			write_SQL_output(outputFilename)
		else:
			print('')
	else: 
		#This will delete old csv/table with same name, then write new ones.
		if sql_table_exists(outputFilename)==True:
			drop = sql.SQL("""DROP TABLE {}; """).format(sql.Identifier(outputFilename))
			xCURSORx.execute(drop)
			logging.debug("Old SQL table-"+outputFilename+" deleted.")
		if str(outputFilename+'.csv') in os.listdir():
			os.remove(str(outputFilename+'.csv'))
			logging.debug("Old "+outputFilename+".csv deleted.")
		if writeToCSV==True:
			make_table(outputFilename)
			write_csv_out(outputBin, outputFilename)
		if writeToSQL==True:
			write_SQL_output(outputFilename)

def voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi):
	"""
	Uses psycopg2 cursor object to make SQL query, using 3 dimensional bounding box, and appends the point with lowest elevation to an ouput list (if there are any points). 
	Use float values as input. 
	Coordinates: X Y Z dimensions become E(easting) N(northing) Z(elevation).
	"""

	query = sql.SQL("""SELECT e, n, z, rgby FROM {} WHERE (n BETWEEN %(nmin)s and %(nmax)s) AND (e BETWEEN %(emin)s AND %(emax)s) AND (z BETWEEN %(zmin)s AND %(zmax)s);""").format(sql.Identifier(inputSQLtable))
	xCURSORx.execute(query, {'nmin':nbinLow, 'nmax':nbinHi, 'emin':ebinLow, 'emax':ebinHi, 'zmin': zbinLow, 'zmax': zbinHi})
	collectedResults = xCURSORx.fetchall()
	if len(collectedResults) > 0:
		logging.debug("Voxel with points\n" + str(collectedResults))
		threshold = (-99999, -99999, 10000000000000, -99999)
		for result in collectedResults:
			alpha, beta, charlie, delta = threshold
			a, b, c, d = result
			if c < charlie:
				threshold = result
		logging.info("Low point " +str(threshold))	
		outputBin.append(threshold)	


########################################
# Spatial Volume, Calculation Functions 

def initializeVars():
	""" Divides (max. value - min. value) of a coordinate column by the binCount. """
	global stepN
	global stepE
	global stepZ
	stepN = gapN/binCount
	stepE = gapE/binCount
	stepZ = gapZ/binCount
	logging.info("Step values calculated")
	
def makeTupleList(binSet, minAA, stepAA, maxAA):
	"""
	Takes coordinate column name (string), minimum coordinate (float), step value (float, see initializeVars()), and max coordinate (float). Returns a list of tuples that are low & high values for one dimension of a bounding box. Each low value is the same as the preceding high value. First low value/last high value are adjusted down/up by a half step value to ensure they contain the min/max coordinate values.

	Example:	makeTupleList("n", 47.653, 2.57825, 54.40)
	"""
	if binSet == "n":
		logging.debug("minAA is %s, stepAA is %s, maxAA is %s"%(minAA, stepAA, maxAA))
		firstCoord = round(minAA-0.5*stepAA, 6)
		secondCoord = round(minAA+0.5*stepAA, 6)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			nbinList.append((firstCoord,secondCoord))
			maxvalue =secondCoord
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
		logging.info("nbinList length:"+ str(len(nbinList)))
		logging.debug("nbin terms"+str(nbinList))

	elif binSet == "e":
		logging.debug("minAA is %s, stepAA is %s, maxAA is %s"%(minAA, stepAA, maxAA))
		firstCoord = round(minAA-0.5*stepAA, 6)
		secondCoord = round(minAA+0.5*stepAA, 6)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			ebinList.append((firstCoord,secondCoord))
			maxvalue =secondCoord
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
		logging.info("ebinList length:"+ str(len(ebinList)))
		logging.debug("ebin terms"+str(ebinList))

	elif binSet == "z":
		logging.debug("minAA is %s, stepAA is %s, maxAA is %s"%(minAA, stepAA, maxAA))
		firstCoord = round(minAA-0.5*stepAA, 6)
		secondCoord = round(minAA+0.5*stepAA, 6)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			#logging.debug("firstcoord: %s \n secondcoord: %s"%(firstCoord, secondCoord))
			zbinList.append((firstCoord,secondCoord))
			maxvalue =secondCoord
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
		logging.info("zbinList length:"+ str(len(zbinList)))
		logging.debug("zbin terms"+str(zbinList))


	else: 
		logging.warning("Bin label error")



##############################################################################
# Main Program

logging.basicConfig(filename='SQLtester.log',level=logging.DEBUG)
logging.info("...\n...\nStarting Voxelizer at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))

conn= startCursor(database, user, pw)

with conn.cursor() as xCURSORx:
	
	fetchParams(inputSQLtable)
	initializeVars()
	makeTupleList("n", minN, stepN, maxN)
	makeTupleList("e", minE, stepE, maxE)
	makeTupleList("z", minZ, stepZ, maxZ)
	if (len(zbinList) != len(ebinList)) or (len(ebinList) != len(nbinList)) or (len(zbinList) != len(nbinList)):
		logging.warning("BinLists are different lengths")

	logging.info(" \n######################################## \nBeginning parsing and selection")
	#This loops thru the entire point cloud, one 'box' at a time, selecting lowest elevation point if any points are found
	for n in nbinList:
			nbinLow = n[0]
			nbinHi = n[1]
			for e in ebinList:
				ebinLow = e[0]
				ebinHi = e[1]
				for z in zbinList:
					zbinLow = z[0]
					zbinHi = z[1]
					voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi)
	logging.info("\n \n Final Output Selection")
	logging.debug(outputBin)


	output_handler()
	conn.commit()

	logging.info("Finished completely at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))




print("working now")