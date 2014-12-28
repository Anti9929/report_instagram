from __future__ import generators 
import MySQLdb
from array import *
from datetime import datetime, time, date, timedelta
from dateutil import parser
from math import modf
import time_converter

def time_or_None(obj):
    """Returns a TIME column as a time object:

      >>> time_or_None('15:06:17')
      datetime.time(15, 6, 17)
      
    Illegal values are returned as None:
 
      >>> time_or_None('-25:06:17') is None
      True
      >>> time_or_None('random crap') is None
      True
   
    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    
    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for FIELD_TYPE.TIME.
    """
    try:
        hour, minute, second = obj.split(':')
        return time(hour=int(hour), minute=int(minute), second=int(second))
    except ValueError:
        return None

def ResultIter(cursor, arraysize=1000):
	while True:
		results = cursor.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result

""" SCRIPT START """

eventId = raw_input('Please provide an event id: ')
minutesBefore = raw_input('Tweets from how many minutes before event start? ')
minutesAfter = raw_input('Until how many minutes after the event? ')

# Get eventdate, starttime, endtime and sport
db = MySQLdb.connect(host="localhost",
					user="root",
					passwd="",
					db="olympics")

db2 = MySQLdb.connect(host="localhost",
					user="root",
					passwd="",
					db="olympics")

# db2 = MySQLdb.connect(host="liapc3.epfl.ch",
# 					user="kempter",
# 					passwd="kempter%pc3",
# 					db="boiadb")

c = db.cursor()
c2 = db2.cursor()
c3 = db.cursor()

c.execute("""SELECT sport, eventDate, startTime, endTime FROM events WHERE id = %s""", (eventId,))
eventArray = c.fetchone()

if eventArray:
	sport = eventArray[0]
	eventDate = eventArray[1]
	startTime = eventArray[2]
	endTime = eventArray[3]
	print(sport)
else:
	print('No valid event id provided')


# Get hashtag of specific sport
c.execute("""SELECT hashtag FROM sports WHERE sport = %s""", (sport,))
eventHashtag = c.fetchone()[0]

if eventHashtag:
	eventHashtag = eventHashtag.rstrip()
else:
	print('Hashtag is empty: Check key relation')

# Read wordlist
file = open("wordlist/emotionalKeywords.txt")
words = list()

while 1:
	line = file.readline()
	if not line:
		break
	line = line.rstrip()
	words.append('text LIKE "%%'+line+'%%"')

# Transform wordlist into SQL
sql_words = ' OR '.join(words)

# Convert time from database to a python time object (not timedelta)
startTime = time_or_None(str(startTime))
endTime = time_or_None(str(endTime))

# Convert date & time into a python datetime object. Adjust the time interval (start - end)
startDateTime = datetime(eventDate.year, eventDate.month, eventDate.day, startTime.hour, startTime.minute) - timedelta(minutes=int(minutesBefore))
endDateTime = datetime(eventDate.year, eventDate.month, eventDate.day, endTime.hour, endTime.minute) + timedelta(minutes=int(minutesAfter))
dateTime = intervalDateTime = startDateTime

# First round extraction: Hashtag + Timeframe + Wordlist

print "Start first phase of extraction"

while(intervalDateTime < endDateTime):
	intervalDateTime = dateTime+timedelta(minutes=1)
	print intervalDateTime
	# Construction of SQL Query
	sql_statement = "SELECT id, text, user, dateTime FROM tweets_olympics WHERE text LIKE %s AND text NOT LIKE '%%RT%%' AND dateTime > %s AND dateTime <= %s AND ("
	sql_statement += sql_words
	sql_statement += ") LIMIT 5"
	
	# Execute query and fetch results
	c2.execute(sql_statement, ('%'+eventHashtag+'%', dateTime, intervalDateTime))
	results = c2.fetchall()
	
	# Insert result into DB
	for result in results:
		print result[0]
		c.execute("""INSERT IGNORE INTO tweet_selection (id, text, user, dateTime) VALUES (%s, %s, %s, %s)""" , (result[0], result[1], result[2], result[3]))
		c.execute("""INSERT IGNORE INTO user_selection (user, firstTweet) VALUES (%s, %s)""", (result[2], result[0]))
	
	# Take next intervall
	dateTime = dateTime+timedelta(minutes=1)

# Second round of extraction: Hashtag + Timeframe + User

print "START Second phase of extraction"

dateTime = intervalDateTime = startDateTime

c.execute("""SELECT user, firstTweet FROM user_selection""")

for result in ResultIter(c):
	user = result[0]
	firstTweet = result[1]
	c2.execute("""SELECT id, text, user, dateTime 
					FROM tweets_olympics 
					WHERE user = %s AND
						  dateTime < %s AND
						  id > %s""", (user, endDateTime, firstTweet))
	results = c2.fetchall()
	for row in results:
		c3.execute("""INSERT IGNORE INTO tweet_selection (id, text, user, dateTime) VALUES (%s, %s, %s, %s)""", (row[0], row[1], row[2], row[3]))

c.close ()

