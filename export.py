import json
from pymongo import MongoClient
import sys

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
    controls = []
    experiments = []
    report_ids = {}

    for measurement in measurements['result']:
        if not measurement['report_id'] in report_ids:
            report_ids[measurement['report_id']] = []
        report_ids[measurement['report_id']].append(measurement)

    # For each report, find if its a control or experiment.
    for report_id, measurements in report_ids.items():
        report = db.reports.find_one({"_id": report_id})
        if report['probe_cc'] == 'NL':
            controls.extend(measurements)
        else:
            experiments.extend(measurements)

    for experiment in experiments:
        closest_control = find_closest(controls, experiment)
        status = truth_table(experiment, closest_control)
        experiment['status'] = status
        bridge = measurement['input']
        output[country][bridge].append(experiment)

if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    main(hashes_filename)

