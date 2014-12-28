# -*- coding: latin-1 -*-
from __future__ import generators 
import string
import MySQLdb
import re
import math

def ResultIter(cursor, arraysize=1000):
	while True:
		results = cursor.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result

# Dictionnary

categoryLookup = dict()

# word, wordnbr, valence mean, valence variance, arousal mean, arousal variance, dominance mean, dominance arousal, word frequency

categoryFile = open('wordlist/emotionalCategories.txt')

while 1:
	line = categoryFile.readline()
	if not line:
		break
	values = line.split('\t')
	categoryLookup[values[0]] = values[1:]

print categoryLookup
print "Establish DB connection"
# Database connection
db = MySQLdb.connect(host="localhost",
					 user="root",
					 passwd="",
					 db="olympics")
c1 = db.cursor()
c2 = db.cursor()

selectStatement = """SELECT * FROM tweets_anew"""
c1.execute(selectStatement)

for result in ResultIter(c1):
	minValue = 100000

	for key, value in categoryLookup.iteritems():
		dist = pow(abs(float(value[0]) - result[1]), 2) + pow(abs(float(value[1])-result[2]), 2) + pow(abs(float(value[2])-result[3]), 2)
		if dist < minValue:
			minValue = dist
			label = key
			print "Distance to label %s is %s" % (label, dist)

	updateStatement = """UPDATE tweets_anew SET label=%s WHERE id=%s"""
	c2.execute(updateStatement, (label, result[0]))
