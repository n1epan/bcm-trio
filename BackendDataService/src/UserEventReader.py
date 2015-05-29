from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from collections import OrderedDict

# Global session parameters
cassandraUsername = 'cassandra'
cassandraPassword = 'cassandra'
cassandraNodeIPs = ['cassandra-useast1b-node1.dev.bosecm.com']
rawCassandraKeyspace = 'bcm_events_raw'
matchedCassandraKeyspace = 'efe'
rawUserColumnFamily = 'bose_jasper_efe'
matchedUserColumnFamily = 'matchefe'

_USE_MATCHED_DATABASE = False


# Establish cassandra session, returns a Cassandra session based on authentication/server arguments
def initializeCassandraSession():
    cluster = Cluster(cassandraNodeIPs, auth_provider=PlainTextAuthProvider(username=cassandraUsername, password=cassandraPassword))
    session = cluster.connect()
    return session

# helper function to query Cassandra to obtain deviceID from a given user ID (BoseID)
# necessary because deviceID is a primary key so we can order easily from it
def getDeviceFromUserID(session, userID):
    # todo add exception handling
    f = session.execute("select deviceid from %s where boseid = '%s'" % (rawUserColumnFamily, userID))
#    for rec in f:
#        print rec.deviceid
    deviceDesc=session.execute("select * from %s where boseid = '%s' limit 10" % (rawUserColumnFamily, userID))
    return deviceDesc[len(deviceDesc)-1].deviceid


def getEventsFromUser(userID, maxNumEvents = 1000):
    #establish Cassandra session
    session = initializeCassandraSession()
    if (_USE_MATCHED_DATABASE):
        session.set_keyspace(matchedCassandraKeyspace)
        queryText = "select * from %s where userid = '%s' order by timestamp desc limit %s" % (matchedUserColumnFamily, userID, maxNumEvents)
    else:
        session.set_keyspace(rawCassandraKeyspace)
        # get deviceID from user ID - necessary because in current schema deviceID is part of primary key, and
        # Cassandra can only sort descending on primary keys
        deviceID = getDeviceFromUserID(session, userID)
        queryText = "select * from %s where deviceid = '%s' order by ts desc limit %s" % (rawUserColumnFamily, deviceID, maxNumEvents)

    # todo - add exception handling in case query fails
    result=session.execute(queryText)

    session.shutdown()
    return result

def getNLastStationsFromUser(userID, maxStations = 3, maxAlbums = 4, maxArtists = 4):
    result = getEventsFromUser(userID)

    stationSet = []

    # Output will be a list of dicts. Each dict entry will have as keys:
    # name: Station name
    # lastPlayedAlbums - list of most recently played albums, in reverse chronological order
    # lastPlayedArtists - list of most recently played artists, in reverse chronological order
    # todo - figure out of there needs to be a 1-1 correspondence between last played albums and artists

    stationsSeenSoFar = []
    for record in result:
        if (_USE_MATCHED_DATABASE):
            station = "Dummy station" # todo - current matched DB doesn't log station/source name
            albumName = "Dummy album"
            artistName = "Taylor Swift"
        else:
            station = record.track['sourceName']
            albumName = record.track['album']
            artistName = record.track['artist']
            # print station

        if station != '':
            # If we've seen this station before, attempt to add album to history for this station
            # Otherwise, add station to list
            if station in stationsSeenSoFar:
                stationIdx = stationsSeenSoFar.index(station)
            else:
                entry = dict()
                entry['name'] = station
                entry['lastPlayedAlbums'] = []
                entry['lastPlayedArtists'] = []

                # update master station list with new entry
                stationIdx = 0
                stationSet.insert(0,entry)
                stationSet = stationSet[0:maxStations]
                # update shadow set of stations seen so far
                stationsSeenSoFar.insert(0,station)
                stationsSeenSoFar = stationsSeenSoFar[0:maxStations]
            # put new station at start of station list
            stationEntry = stationSet[stationIdx]
            # now check if this album and artists among the last played
            if albumName != '':
                if albumName not in stationEntry['lastPlayedAlbums'] and len(stationEntry['lastPlayedAlbums']) < maxAlbums:
                    stationEntry['lastPlayedAlbums'].insert(0,albumName)
                    stationEntry['lastPlayedAlbums'] = stationEntry['lastPlayedAlbums'][0:maxAlbums]

            if artistName != '':
                if artistName not in stationEntry['lastPlayedArtists'] and len(stationEntry['lastPlayedArtists']) < maxArtists:
                    stationEntry['lastPlayedArtists'].insert(0,artistName)
                    stationEntry['lastPlayedAlbums'] = stationEntry['lastPlayedAlbums'][0:maxArtists]

            # store back
            stationSet[stationIdx] = stationEntry
    return stationSet

