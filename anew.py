# -*- coding: latin-1 -*-
from __future__ import generators 
import string
import MySQLdb
import re

def ResultIter(cursor, arraysize=1000):
	while True:
		results = cursor.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result

# Dictionnary

anewLookup = dict()

# word, wordnbr, valence mean, valence variance, arousal mean, arousal variance, dominance mean, dominance arousal, word frequency

anewLexicon = open('wordlist/ANEWLexicon.txt')

while 1:
	line = anewLexicon.readline()
	if not line:
		break
	values = line.split('\t')
	anewLookup[values[0]] = values[2:8]

print "Establish DB connection"
# Database connection
db = MySQLdb.connect(host="localhost",
					 user="root",
					 passwd="",
					 db="olympics")
c1 = db.cursor()
c2 = db.cursor()

sqlStatement = """SELECT id, text FROM tweets_olympics"""

c1.execute(sqlStatement)

print "Iterate through results"

for result in ResultIter(c1):
	# normalization
	tokens = re.findall(r"\w+(?:[-’]\w+)*|’|[-.(]+|\S\w*", result[1])

	i = 0

	for token in tokens:
		valence = arousal = dominance = 0
		try:
			valence = valence + float(anewLookup[token][0])
			arousal = arousal + float(anewLookup[token][2])
			dominance = dominance + float(anewLookup[token][4])
			i = i + 1
		except KeyError:
			continue

		if(valence != 0 and arousal != 0 and dominance != 0):
			insertStatement = """INSERT IGNORE INTO tweets_anew (id, valence, arousal, dominance) VALUES(%s, %s, %s, %s)"""
			c2.execute(insertStatement, (result[0], valence / i, arousal / i, dominance / i))



