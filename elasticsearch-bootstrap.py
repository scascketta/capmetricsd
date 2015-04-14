#!/usr/bin/env python

import glob
import json
import sys

import arrow
import requests
import elasticsearch
import elasticsearch.helpers

def color(name, text, bold=False):
    codes = {'red': 31, 'green': 32, 'yellow': 3, 'blue': 34, 'magenta': 35, 'cyan': 36, 'white': 37}
    return '\033[{}m{}\033[0m'.format(codes[name], text)


def headline(text):
    splash = '=' * (79 - len(text))
    print(color('green', '{} {}'.format(text, splash)))
    sys.stdout.flush()


def show_error(text):
    print(color('red', '=' * 79))
    print(color('red', text))
    print(color('red', '=' * 79))
    sys.stdout.flush()


def setup_index():
    index = {
        "mappings": {
            "vehicle_position": {
                "properties": {
                    "location": {
                        "type": "geo_point"
                    },
                    "route_id": {
                        "type": "string"
                    },
                    "speed": {
                        "type": "long"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "dateOptionalTime"
                    },
                    "trip_id": {
                        "type": "string"
                    },
                    "vehicle_id": {
                        "type": "string"
                    }
                }
            }
        }
    }
    res = requests.post('http://localhost:9200/capmetrics', data=json.dumps(index))
    if not res.ok:
        show_error(res.json())
    res.raise_for_status()


def add_vehicle_docs():
    es = elasticsearch.Elasticsearch()

    for fname in glob.glob('data/vehicle_positions/json/*'):
        headline('{} {} {}'.format('=' * 50, fname, '=' * 50))

        with open(fname, 'r') as fh:
            data = json.loads(fh.read())

        actions = []

        for doc in data:
            # vehicle id + unix timestamp seems like a good doc id
            # without this, imports wouldn't be idempotent
            id = '{}-{}'.format(doc['vehicle_id'], arrow.get(doc['timestamp']).timestamp)

            actions.append({
                '_index': 'capmetrics',
                '_type': 'vehicle_position',
                '_id': id,
                '_source': doc,
            })

        print('Running {} bulk actions'.format(len(actions)))

        success, errors = elasticsearch.helpers.bulk(es, actions)

        print('{:,} succeeded, {:,} failed'.format(success, len(errors)))
        for error in errors:
            show_error(error)

if __name__ == '__main__':
    setup_index()
    add_vehicle_docs()

