import json
from pymongo import MongoClient
from bson.json_util import dumps
import sys
import pprint

def find_closest(controls, experiment):
    return min(controls, key=lambda x: abs(x['start_time'] - experiment['start_time']))

def truth_table(experiment, control):
    result_experiment = experiment['success']
    result_control = control['success']

    if result_experiment == True and result_control == True:
        return "ok"
    elif result_experiment == True and result_control == False:
        return "inconsistent"
    elif result_experiment == False and result_control == True:
        return "blocked"
    elif result_experiment == False and result_control == False:
        return "offline"

def get_hashes(hashes_filename):
    """ Get hashes from filename input"""
    hashes = open(hashes_filename).readlines()
    hashes = [h.rstrip() for h in hashes]
    return hashes

def main(hashes_filename):
    hashes = get_hashes(hashes_filename)

    # Connect to database
    client = MongoClient('127.0.0.1', 27017)
    db = client.ooni

    # Find measurements we are interested in. 
    measurements = db.measurements.aggregate([{"$match": {"input": {"$in": hashes}}}])


    # Populate an auxiliary variable with the measurements of a report
    report_ids = {}
    for measurement in measurements['result']:
        if not measurement['report_id'] in report_ids:
            report_ids[measurement['report_id']] = []
        report_ids[measurement['report_id']].append(measurement)

    # For each report, find if its a control or experiment.
    controls = []
    experiments = {}
    for report_id, measurements in report_ids.items():
        report = db.reports.find_one({"_id": report_id})
        country = report['probe_cc']
        if country == 'NL':
            controls.extend(measurements)
        else:
            if country not in experiments:
                experiments[country] = []
            experiments[country].extend(measurements)

    # Generate the output
    output = {}
    for country, measurements in experiments.items():
        if country not in output:
            output[country] = {}
        for measurement in measurements:
            closest_control = find_closest(controls, measurement)
            status = truth_table(measurement, closest_control)
            measurement['status'] = status
            bridge = measurement['input']
            if bridge not in output[country]:
                output[country][bridge] = []
            output[country][bridge].append(measurement)
    return output

if __name__ == "__main__":
    hashes_filename = sys.argv[1]
    output = main(hashes_filename)
    output_filename = sys.argv[2]
    print dumps(output)
