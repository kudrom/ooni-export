import json
from pymongo import MongoClient
import sys

def closest(

def main(hashes_filename):
    # XXX: Catch exceptions
    # Read bridge hashes from file
    hashes = open(hashes_filename).readlines()
    hashes = [h.rstrip() for h in hashes]
    
    # Connect to database
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    # Find measurements we are interested in. 
    measurements = db.measurements.aggregate([{"$match": {"input": {"$in": hashes}}}])

    # Find control and experiment reports
    control = []
    experiments = []
    report_ids = set()

    for measurement in measurements['result']:
        report_ids.add(measurement['report_id'])

        # For each report, find if its a control or experiment.
        for report_id in report_ids:
            report = db.reports.find_one({"_id": report_id})
            if report['probe_cc'] == 'NL':
                control.append([report)
            else:
                experiments.append(report)

    # Now find which measurements are control and which are
    # experiments.
    for experiment in experiments:


if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    main(hashes_filename)

