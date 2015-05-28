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

def getNLastStationsFromUser(userID, maxStations = 3):
    result = getEventsFromUser(userID)

    stationSet = OrderedDict()

    for record in result:
        if (_USE_MATCHED_DATABASE):
            station = "Dummy station" # todo - current matched DB doesn't log station/source name
            albumName = "Dummy album"
        else:
            station = record.track['sourceName']
            albumName = record.track['album']
            # print station

        if station != '':
            # If we've seen this station before, attempt to add album to history for this station
            # Otherwise, add station to map
            albumSet = stationSet.get(station, OrderedDict())
            if albumName != '':
               albumSet[albumName] = albumSet.get(albumName,0)+1
            stationSet[station] = albumSet

     #   todo - just return list of recent stations for now
    if (len(stationSet) > maxStations):
        return stationSet[0:maxStations]
    else:
        return stationSet

