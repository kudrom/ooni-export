import json
from pymongo import MongoClient
import sys
import pprint

def find_closest(controls, experiment):
    return min(controls, key=lambda x: abs(x['start_time'] - experiment['start_time']))

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
    experiments = {}
    report_ids = {}

    for measurement in measurements['result']:
        if not measurement['report_id'] in report_ids:
            report_ids[measurement['report_id']] = []
        report_ids[measurement['report_id']].append(measurement)

    # For each report, find if its a control or experiment.
    for report_id, measurements in report_ids.items():
        report = db.reports.find_one({"_id": report_id})
        country = report['probe_cc']
        if country == 'NL':
            controls.extend(measurements)
        else:
            if country not in experiments:
                experiments[country] = []
            experiments[country].extend(measurements)

    output = {}
    for country, measurements in experiments.items():
        for measurement in measurements:
            closest_control = find_closest(controls, measurement)
            print "===================="
            print "Experiment:", measurement
            print "Control:", closest_control
            print "===================="
            """
            status = truth_table(experiment, closest_control)
            experiment['status'] = status
            bridge = measurement['input']
            output[country][bridge].append(experiment)
            """

if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    main(hashes_filename)

