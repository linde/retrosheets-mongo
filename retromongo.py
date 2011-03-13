#!/usr/bin/python
#
#   retromongo.py -  a script to load retrosheet play-by-play data to mongodb
# 
#   see:
#   http://www.retrosheet.org/game.htm
#   http://www.mongodb.org/
#
#   Copyright 2011 Steven Linde
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# 

import sys
import glob
import os
import re
import json
import logging
import csv

from optparse import OptionParser
from pymongo import Connection

# from pudb import set_trace; set_trace()    


class EventFileParser:

    def __init__(self, collection):
        self.collection = collection
        self.curGame = {}


    def processDirectory(self, dir):

        globPattern = "%s/*.EV*" % dir
        logging.info("processing games in: %s ...." % globPattern)

        for fileName in glob.glob(globPattern):
            self.parse(fileName)


    def parse(self, fileName):

        fileBase = os.path.basename(fileName)
        year, team = re.findall(r"(^\d{4})(\w{3}).+?$", fileBase)[0]
        
        logging.debug("start: %s for %s from %s" % (year, team, fileName))
        
        csvReader = csv.reader( open(fileName, "r") )
        for fields in csvReader:
            op = getattr(self, fields[0], self.unsupported)
            op(fields)

        logging.debug("finished: %s for %s" % (year, team))


    def id(self, fields):

        # sample line: id,SFN200904070

        if ( self.curGame ):
            self.store(self.curGame)
            logging.debug("stored game: %s" % self.curGame['_id'])

        game = {}
        game['_id'] = fields[1]
        game['home'], game['year'], game['month'], game['day'], game['seq'] = \
            re.findall(r"(^\w{3})(\d{4})(\d{2})(\d{2})(\d{1})$", fields[1])[0]
        game['info'] = {}
        game['starts'] = {
            'home' : [],
            'away' : []
            }
        game['events'] = []
        game['data'] = []

        self.curGame = game

    # end of id


    def version(self, fields):
        # sample line: version,2
        self.curGame['version'] = fields[1]


    def info(self, fields):
        # sample line: info,site,SFO03
        self.curGame['info'][fields[1]] = fields[2]

    def start(self, fields):
        # sample line: start,howar001,"Ryan Howard",0,4,3

        start = {
            'id' : fields[1],
            'battingOrder' : fields[4],
            'position' : fields[5]
            }

        loc = 'home' if fields[3] == "0" else 'away'
        self.curGame['starts'][loc].append(start) 

        # TODO clean this up so its easier to figure out who is
        # playing during any given activity


    def play(self, fields):
        
        # sample line: play,6,1,bondb001,02,CFX,HR/9.3-H;2-H;1-H

        playStr = fields[6]
        if playStr.find('/') < 0:
            playStr += "/"
                
        playCode, rest = playStr.split('/', 1)

        if rest.find('.') < 0:
            rest = rest + '.'

        modifier, basesAdvanced = rest.split('.', 1)

        playDetails = {
            'raw' : fields[6],
            'code' : playCode,
            'modifier' : modifier,
            'basesAdvanced' : basesAdvanced
            }

        logging.debug("checking: %s -> %s" % (fields[6], json.dumps(playDetails)))

        play = {
            'type' : 'play',
            'inning' : int(fields[1]),
            'half' : 'top' if fields[2] == "0" else 'bottom',
            'playerId' : fields[3],
            'countOnBatter' : {
                },
            'sequenceOfPitches' : list(fields[5]),
            'details' : playDetails
            }
        
        if re.match("^\d\d$", fields[4]):
            play['countOnBatter'] = {
                'balls' : int(fields[4][0]),
                'strikes' : int(fields[4][1])
                }

        self.curGame['events'].append( play )


    def com(self, fields):

        # sample line: com,"$Career homer 587 to pass Frank Robinson for 4th all-time"

        # or sample multi-line comment:
        # 
        # com,"$Hall caught in rundown while Winn advanced to 3B; both players"
        # com,"ended up on 3B and Winn is tagged out; Hall thought he was the one"
        # com,"who was out and stepped off the bag and is tagged out"
        # 

        if not self.curGame['events'] or self.curGame['events'][-1]['type'] != 'comment':
            newEvent = {
                'type' : 'comment',
                'content' : []
            }
            self.curGame['events'].append( newEvent )

        self.curGame['events'][-1]['content'].append(unicode(fields[1], errors='replace'))


    def badj(self, fields):
        
        # sample line: badj,everc001,L

        adjustment = {
            'type' : 'battingAdjustment',
            'player' : fields[1],
            'hand' : fields[2]
            }
        
        self.curGame['events'].append( adjustment )


    def padj(self, fields):

        # sample line: padj,harrg001,L

        adjustment = {
            'type' : 'pitchingAdjustment',
            'player' : fields[1],
            'hand' : fields[2]
            }
        
        self.curGame['events'].append( adjustment )


    def ladj(self, fields):

        # sample line: ladj,0,9
        # linde: guessing here what this should mean...

        ladj = {
            'type' : 'ladj',
            'description' : 'batting out of order',
            'firstField' : fields[1],
            'secondField' : fields[2]
            }
        
        self.curGame['events'].append( ladj )


    def sub(self, line):
        logging.debug("sub: storing substition: %s" % line)


    def data(self, fields):

        # sample line: data,er,fyhrm001,0
        # only data as of now is earned runs, so supporting directly

        data = {
            'player' : fields[2],
            'earnedRuns' : fields[3]
            }
        self.curGame['data'].append(data)


    def unsupported(self, line):
        logging.warn("skipping unsupported op: %s" % line)


    def store(self, game):

        try:
            self.collection.insert(game)
            
        except:
            logging.warn("problems saving game: %s" %  game.id )


### end of EventFileParser

class TeamsFileParser:

    def __init__(self, collection):
        self.collection = collection


    def processDirectory(self, dir):
        logging.info("processing teams...")

        for file in glob.glob("%s/TEAM*" % dir ):
    
            year = re.findall(r"^TEAM(\d{4})$", os.path.basename(file))[0]

            csvReader = csv.reader( open(file, "r") )
            for line in csvReader:

                if len(line) != 4:
                    continue
        
                code,league,place,team = line
                key = code + year
                team = {
                    '_id' : key,
                    'teamcode' : code,
                    'year' : year,
                    'league' : league,
                    'place' : place,
                    'team' : team
                    }

                logging.debug( json.dumps( team ) )
                self.store(team)


    def store(self, team):
        try:
            self.collection.insert(team)
        except:
            logging.warn("problems saving team: %s" %  team['_id'] )



### end of TeamsFileParser



class RosterFileParser:

    def __init__(self, collection):
        self.collection = collection


    @staticmethod
    def getRosterKey(team, year, playerId):
        return team + ":" + year + ":" + playerId
    

    def processDirectory(self, dir):
        logging.info("processing rosters...")

        for file in glob.glob("%s/*.ROS" % dir):

            team, year = re.findall(r"(^\w{3})(\d{4}).+?$", os.path.basename(file))[0]

            csvReader = csv.reader( open(file, "r") )
            for line in csvReader:
              
                playerId, last, first, bats, throws, team, position = line               

                key = RosterFileParser.getRosterKey(team, year, playerId)

                entry = {
                    '_id' : key,
                    'team' : team,
                    'year' : year,
                    'id' : playerId,
                    'last' : last,
                    'first' : first,
                    "bats" : bats,
                    "throws" : throws,
                    "position" : position
                    }

                logging.debug( json.dumps( entry ) )
                self.store(entry)


    def store(self, player):

        try:
            self.collection.insert(player)
            
        except:
            logging.warn("problems saving roster entry for: %s" %  player['_id'] )


### end of RosterFileParser


def main():

    parser = OptionParser()
    parser.add_option("-H", "--host", dest="host", default="localhost",help="mongodb host")
    parser.add_option("-p", "--port", dest="port", default=27017, help="mongodb port")
    parser.add_option("-d", "--db", dest="db", default="retromongo", help="mongodb database")
    parser.add_option("-i", "--init-db", dest="initDb", action="store_true", default=False, help="initialize mongodb database")
    parser.add_option("-D", "--dir", dest="dir", help="retrosheet extract directorye")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="no output except errors")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="verbose output")

    (options, args) = parser.parse_args()

    if options.quiet and options.verbose:
        logging.error("cannot be both verbose and quiet")
        parser.print_help()
        quit(-2)

    elif options.quiet:
        logging.basicConfig(level=logging.WARN)

    elif options.verbose:
        logging.basicConfig(level=logging.DEBUG)

    else:
        logging.basicConfig(level=logging.INFO)

    if not options.dir:
        logging.error("you must specify a directory")
        parser.print_help()
        quit(-1)

    connection = Connection(options.host, options.port)
    db = connection[options.db]
    
    if not db:
        logging.error("couldnt connect to: %s:%d/%s" % (options.host, options.port, options.db))
        quit(-1)
    
    if options.initDb:
        connection.drop_database(options.db) 
        logging.info("initialed retrosheets db: %s:%d/%s" %  (options.host, options.port, options.db))

    teamFileParser = TeamsFileParser(db.teams)
    teamFileParser.processDirectory(options.dir)
                
    rosterFileParser = RosterFileParser(db.rosters)
    rosterFileParser.processDirectory(options.dir)
        
    eventFileParser = EventFileParser(db.games)
    eventFileParser.processDirectory(options.dir)

    logging.info("done!")
    return 0


if __name__ == '__main__':
    sys.exit( main() )
