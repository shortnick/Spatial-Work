###################################################################
#PythonPostGRES
#
#
# Status 12-2-17: trying to replace iteration loops with lists of zipped tupples and simple list comprehension
# Status 12-21-17: weirdness-> zbin is 9 terms long, not 5 like nbin and ebin @ 5 ea., plus division isn't being done right for step values?
# Status 12-26-17: selector fixed, problem of the day: text input into SQL command, make table
# Status 1-2-18: Fully functional, new items: nope[functionalize the initial fetch], use the sql.SQL on selectors,
	#group all input variables together (towards an input file), make table creation responsive to presence/absence
	#of old files
# Style guides: https://www.python.org/dev/peps/pep-0008/, https://www.python.org/dev/peps/pep-0484/
# Documentation: https://devguide.python.org/documenting/#documenting
################################################################################


import logging
#see loggin cursor http://initd.org/psycopg/docs/extras.html#module-psycopg2.extras
import datetime

import psycopg2
from psycopg2 import sql

""" Input vars to add
input sql table
output sql table
	- relable option
	- overwrite option
logging specifics

"""
#### Input Variables
binCount = 4
binMax = abs(binCount)

#Range & Step variables
maxN = -9 
minN = -9
countN = -9
gapN = -9
maxE = -9
minE = -9
countE = -9
gapE = -9
maxZ = -9
minZ = -9 
countZ = -9
gapZ = -9
stepN = -9
stepE= -9
stepZ= -9

#Selection Bin Variables
nbinLow = -9
nbinHi = -9
ebinLow = -9
ebinHi = -9
zbinLow = -9
zbinHi = -9
nbinList = []
ebinList = [] 
zbinList = []
outputBin = []

# Functions

def startCursor(dbase, user_name, pw):
	"""
	Uses psycopg2.connect(), returning a cursor object with an open POSTgreSQL connection. Use strings for inputs. 
	"""
	loginString = "dbname="+dbase+" user="+user_name+" password="+pw
	logging.info("Database Connected at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))
	return psycopg2.connect(loginString)

def fetchParams():
	"""
	Cursor retrieves important parameters of E, N, Z coordinate columns from SQL table.
	"""
	#sql.SQL isn't gonna handle passing in the database.schema.table as a parameter. There is an AsIs() function, but needs some call/response error checking 
	global maxN, minN, countN, gapN
	global maxE, minE, countE, gapE
	global maxZ, minZ, countZ, gapZ
	
	query = sql.SQL("""SELECT max(n), min(n), count(n), (max(n)-min(n)), max(e), min(e), count(e), (max(e)-min(e)), max(z), min(z), count(z), (max(z)-min(z)) FROM quest.public.ptsTiny;""")
	
	xCURSORx.execute(query)
	maxN, minN, countN, gapN, maxE, minE, countE, gapE, maxZ, minZ, countZ, gapZ = (xCURSORx.fetchall()[0])
	logging.debug(" \n maxN = %s, minN = %s, countN = %s,  \n maxE = %s, minE = %s, countE = %s,  \n maxZ = %s, minZ = %s, countZ = %s"%(maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ))
	if countN != countZ or countZ != countE:
		logging.warning("Record counts return mismatched numbers.")


def initializeVars():
	""" Divides (max. value - min. value) of a coordinate column by the binCount. """
	global stepN
	global stepE
	global stepZ
	stepN = gapN/binCount
	stepE = gapE/binCount
	stepZ = gapZ/binCount
	logging.info("Bins Initialized")
	
def makeTupleList(binSet, minAA, stepAA, maxAA):
	"""
	Takes coordinate column name (string), minimum coordinate (float), step value (float, see initializeVars()), and max coordinate (float). Returns a list of tuples that are low & high values for a bounding box. Each low value is the same as the preceding high value. First low value/last high value are adjusted down/up by a half step value to ensure they contain the min/max coordinate values.

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

def voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi):
	"""
	Uses psycopg2 cursor object to make SQL query, using 3 dimensional bounding box, and appends the point with lowest elevation to an ouput list (if there are any points). Use float values as input. Coordinates: X Y Z dimensions become E(easting) N(northing) Z(elevation).

	"""

	xCURSORx.execute("""SELECT e, n, z, rgby FROM ptsTiny WHERE (n BETWEEN %(nmin)s and %(nmax)s) AND (e BETWEEN %(emin)s AND %(emax)s) AND (z BETWEEN %(zmin)s AND %(zmax)s);""", {'nmin':nbinLow, 'nmax':nbinHi, 'emin':ebinLow, 'emax':ebinHi, 'zmin': zbinLow, 'zmax': zbinHi})
	collectedResults = xCURSORx.fetchall()
	if len(collectedResults) > 0:
		logging.debug("Voxel with points\n" + str(collectedResults))
		threshold = (-99999, -99999, 1000000000000, -99999)
		for q in collectedResults:
			alpha, beta, charlie, delta = threshold
			a, b, c, d = q
			if c < charlie:
				threshold = q
		logging.info("Low point " +str(threshold))	
		outputBin.append(threshold)	

def makeSQLtable(table_name):
	"""
	Makes SQL call to drop existing table with this name and create a new one with E, N, Z, color columns. Use string for input.
	"""
	new_table = sql.SQL("""DROP TABLE {}; CREATE TABLE {}
	    (
	      E double precision NOT NULL,
	      N double precision NOT NULL,
	      Z double precision NOT NULL,
	      RGBY character varying(50)
	    );""").format(sql.Identifier(table_name), sql.Identifier(table_name))
	    

	logging.debug("SQL: "+str(new_table))
	xCURSORx.execute(new_table)
	logging.info("Made table "+table_name)

def writeOutput2SQL(table_name, selectedpoints):
	"""
	Uses first input to access SQL table, inserting a row for each tuple in the the second input, using psycopg2 cursor.  
	"""
	logging.debug("Writing results to SQL table " +table_name)
	for point in selectedpoints:
		east, north, up, color = point
		a = sql.SQL("INSERT INTO {tbl} (E, N, Z, RGBY) VALUES (%(east)s, %(north)s, %(up)s, %(color)s);").format(tbl=sql.Identifier(table_name))
		b= ({'east':east, 'north':north, 'up':up, 'color':color})
		#use sql.SQL to write a better logging statement
		logging.debug("SQL: "+str(a)+str(b))
		xCURSORx.execute(a,b)

	logging.info("Output to table " +table_name+ " complete")





# ================================================================================================
# Begin Program



logging.basicConfig(filename='VoxelizerDebugs.log',level=logging.DEBUG)



conn = startCursor('quest', 'basic', 'postgrescorvus')


with conn.cursor() as xCURSORx:
	
	fetchParams()
	
	initializeVars()

	makeTupleList("n", minN, stepN, maxN)
	makeTupleList("e", minE, stepE, maxE)
	makeTupleList("z", minZ, stepZ, maxZ)
	if (len(zbinList) != len(ebinList)) or (len(ebinList) != len(nbinList)) or (len(zbinList) != len(nbinList)):
		logging.warning("BinLists are different lengths")


	logging.info("Beginning parsing and selection")
	#This loops thru all permutations of 3 dimensions, one 'bin' at a time
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
	
	makeSQLtable("VoxelizerOutput")
	
	writeOutput2SQL("VoxelizerOutput", outputBin)
	
	conn.commit()

	logging.info("Finished completely at "+datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"))