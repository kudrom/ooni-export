import json
from pymongo import MongoClient
import sys

def main(hashes_filename):
    hashes = open(hashes_filename).readlines()
    # XXX: Catch exceptions
    hashes = [h.rstrip() for h in hashes]
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    # Populate the reports with the reports we're interested in
    reports = []
    for h in hashes:
        measurements = db.measurements.find({"input": h})
        for m in measurements:
            report = db.reports.find({"_id": m['report_id']})
            for r in report:
                reports.append(r)

    # Find control reports



if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    main(hashes_filename)

