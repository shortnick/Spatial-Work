###################################################################
#PythonPostGRES
#
#
# Status 12-2-17: trying to replace iteration loops with lists of zipped tupples and simple list comprehension
# Status 12-21-17: weirdness-> zbin is 9 terms long, not 5 like nbin and ebin @ 5 ea., plus division isn't being done right for step values?
#
#

################################################################################
#from __future__ import division
import psycopg2
import logging


binCount = 4
binMax = abs(binCount)


#Range & Step variables
maxN = -9 
minN = -9
gapN = -9
maxE = -9
minE = -9
gapE = -9
maxZ = -9
minZ = -9 
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

# Functions

def initialize():
	global stepN
	global stepE
	global stepZ
	stepN = gapN/binCount
	stepE = gapE/binCount
	stepZ = gapZ/binCount
	ratioN = str(stepN/gapN)
	ratioE = str(stepE/gapE)
	ratioZ = str(stepZ/gapZ)
	logging.info("Bins Initialized")
	logging.debug("N step: "+str(stepN)) #, " Ratio of step to gap: " + str(stepN/gapN)
	logging.debug("E step: "+str(stepE)) #, " Ratio of step to gap: " + str(stepE/gapE))
	logging.debug("Z step: "+str(stepZ)) #, " Ratio of step to gap: " + str(stepZ/gapZ))

def makeTupleList(binSet, minAA, stepAA, maxAA):
	if binSet == "n":
		firstCoord = round(minAA-0.5*stepAA, 6)
		secondCoord = round(minAA+0.5*stepAA, 6)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			nbinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
		logging.info("nbinList length:"+ str(len(nbinList)))
		logging.debug("nbin terms"+str(nbinList))

	elif binSet == "e":
		firstCoord = round(minAA-0.5*stepAA, 6)
		secondCoord = round(minAA+0.5*stepAA, 6)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			ebinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
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
			logging.debug("firstcoord: %s \n secondcoord: %s"%(firstCoord, secondCoord))
			zbinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
		logging.info("zbinList length:"+ str(len(zbinList)))
		logging.debug("zbin terms"+str(zbinList))


	else: 
		logging.warning("Bin label error")

def voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi):
	maniP.execute("""SELECT e, n, z, rgby FROM ptsTiny WHERE (n BETWEEN %(nmin)s and %(nmax)s) AND (e BETWEEN %(emin)s AND %(emax)s) AND (z BETWEEN %(zmin)s AND %(zmax)s);""", {'nmin':nbinLow, 'nmax':nbinHi, 'emin':ebinLow, 'emax':ebinHi, 'zmin': zbinLow, 'zmax': zbinHi})
	if len(maniP.fetchall()) > 0:
		logging.debug("Points selected--"+str(maniP.fetchall()))





# ======================================================================================================================
# Begin Program



logging.basicConfig(filename='VoxelizerDebugs.log',level=logging.DEBUG)


try:
	conn = psycopg2.connect("dbname='quest' user='basic' password='postgrescorvus'")
	logging.info("Database Connected")

	with conn.cursor() as maniP:

		maniP.execute(
			"""(select (max(n)-min(n)),
			(max(e)-min(e)),
			(max(z)-min(z))
			from quest.public.ptsTiny)""")
		gapN, gapE, gapZ = (maniP.fetchall()[0])
		#why can't (maniP.fetchall()[0]) be printed? it's 'out of range'
		logging.debug("gapN = %s, gapE = %s, gapZ =%s"%(gapN, gapE, gapZ))
		maniP.execute(
			"""select max(n), min(n), count(n), max(e), min(e), count(e), max(z), min(z), count(z) FROM quest.public.ptsTiny"""
			)
		maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ = (maniP.fetchall()[0])
		logging.debug(" \n maxN = %s, minN = %s, countN = %s,  \n maxE = %s, minE = %s, countE = %s,  \n maxZ = %s, minZ = %s, countZ = %s"%(maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ))

	
	if countN != countZ or countZ != countE:
		logging.warning("Record counts return mismatched numbers.")


	initialize()
	

	makeTupleList("z", minZ, stepZ, maxZ)
	

except:
	logging.warning("Unable to connect to the database")

