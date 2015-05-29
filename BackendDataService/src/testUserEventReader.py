#!/usr/bin/python
import sys
from UserEventReader import getEventsFromUser, getNLastStationsFromUser
import sys

def main(argv):
    if (len(argv)) == 0:
        # device: D05FB8A7B560
        user = 31092
        #user = 8776
    else:
        user = argv[0]

    print getNLastStationsFromUser(user, 3)



if __name__ == '__main__':
     main(sys.argv[1:])