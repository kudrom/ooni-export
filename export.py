import json
import pymongo
import sys

def main(hashes_filename):
    hashes = open(hashes_filename).readlines()
    # XXX: Catch exceptions
    hashes = [h.rstrip() for h in hashes]
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    measurements = []
    for h in hashes:
        measurements.append(db.measurements.find({"input": h}))
    print measurements

if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    main(hashes_filename)

