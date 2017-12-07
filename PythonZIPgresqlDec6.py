################################################################################
#PythonPostGRES
#
#
# Status 12-2-17: trying to replace iteration loops with lists of zipped tupples and simple list comprehension
#
#
#

################################################################################

import psycopg2
import types 
import itertools
import numpy as np

binCount = 6
binMax = binCount


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

def initialize():
	global stepN
	global stepE
	global stepZ
	stepN = gapN/binCount
	stepE = gapE/binCount
	stepZ = gapZ/binCount
	print("Initialized")

def makeTupleList(binSet, minAA, stepAA, maxAA):
	if binSet == "n":
		#print(minAA, stepAA, maxAA)
		firstCoord = minAA-0.5*stepAA
		secondCoord = minAA+0.5*stepAA
		#print(firstCoord, secondCoord)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			nbinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
			#print(maxvalue)
		#print(nbinList[:10])

		#print(len(nbinList))
	elif binSet == "e":
		#print(minAA, stepAA, maxAA)
		firstCoord = minAA-0.5*stepAA
		secondCoord = minAA+0.5*stepAA
		#print(firstCoord, secondCoord)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			ebinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA

	elif binSet == "z":
		#print(minAA, stepAA, maxAA)
		firstCoord = minAA-0.5*stepAA
		secondCoord = minAA+0.5*stepAA
		#print(firstCoord, secondCoord)
		maxvalue = 0
		while maxvalue <= maxAA+0.51*stepAA:
			zbinList.append((firstCoord,secondCoord))
			maxvalue =abs(secondCoord)
			firstCoord = secondCoord
			secondCoord = secondCoord+stepAA
	else: 
		print("Bin label error")

def voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi):
	maniP.execute("""SELECT e, n, z, rgby FROM ptsTiny WHERE (n BETWEEN %(nmin)s and %(nmax)s) AND (e BETWEEN %(emin)s AND %(emax)s) AND (z BETWEEN %(zmin)s AND %(zmax)s);""", {'nmin':nbinLow, 'nmax':nbinHi, 'emin':ebinLow, 'emax':ebinHi, 'zmin': zbinLow, 'zmax': zbinHi})


#connect to PostGRES db
# Connect to an existing database
try:
	conn = psycopg2.connect("dbname='quest' user='basic' password='postgrescorvus'")
	print("Connected")
except:
	print("Unable to connect to the database")

with conn.cursor() as maniP:
	maniP.execute(
		"""(select ((max(n)-min(n))/(count(n)/20)) as ngap,
		((max(e)-min(e))/(count(n)::numeric/20)) as egap,
		((max(z)-min(z))/(count(n)::numeric/20)) as zgap
		from quest.public.ptsTest)""")
	gapN, gapE, gapZ = (maniP.fetchall()[0])
	maniP.execute(
		"""select max(n), min(n), count(n), max(e), min(e), count(e), max(z), min(z), count(z) FROM quest.public.ptsTiny"""
		)
	maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ = (maniP.fetchall()[0])
	print(maxN, minN, countN, maxE, minE, countE, maxZ, minZ, countZ)
	
	if countN != countZ or countZ != countE:
		print("Error: record counts return mismatched numbers.")


	initialize()

	makeTupleList("n", minN, stepN, maxN)
	makeTupleList("e", minE, stepE, maxE)
	makeTupleList("z", minZ, stepZ, maxZ)
	print("N:"+str(nbinList[:10])+", "+str(nbinList[-10:]))
	print("E:"+str(ebinList[:10])+", "+str(ebinList[-10:]))
	print("Z:"+str(zbinList[:10])+", "+str(zbinList[-10:]))

	for n in nbinList:
			#interpret tuple for nbin
			nbinLow = n[0]
			nbinHi = n[1]

			for e in ebinList:
				ebinLow = e[0]
				ebinHi = e[1]
			#interpret tuple for ebin
				for z in zbinList:
					zbinLow = z[0]
					zbinHi = z[1]
					voxelSelector(nbinLow, nbinHi, ebinLow, ebinHi, zbinLow, zbinHi)
					###print(maniP.fetchall())
					
					#outter select for lowest point
					#inner select for the three tuples
					#append outter select to new table