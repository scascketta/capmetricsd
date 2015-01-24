#!/usr/bin/env python
import rethinkdb as r
import geojson as gj
import arrow

import sys
import time


def make_feature(vehicle, id):
    point = gj.Point(tuple(vehicle['location']['coordinates']))
    time = arrow.get(vehicle['timestamp']).to('America/Chicago')
    f = {
        'geometry': point,
        'properties': {'time': time.isoformat()},
        'id': id
    }
    return gj.Feature(**f)


def make_featcoll(vehicle_id, limit):
    docs = get_vehicle_positions(vehicle_id, limit)

    features = []
    for ind, doc in enumerate(docs):
        features.append(make_feature(doc, ind))

    feat_coll = gj.FeatureCollection(features)

    start = arrow.get(docs[0]['timestamp']).to('America/Chicago').isoformat()
    end = arrow.get(docs[-1]['timestamp']).to('America/Chicago').isoformat()
    fname = 'FC_{vid}@{start}<-->{end}.geojson'.format(vid=vehicle_id, start=start, end=end)
    with open(fname, 'w') as f:
        gj.dump(feat_coll, f)


def make_geo_coll(vehicle_id, limit):
    docs = get_vehicle_positions(vehicle_id, limit)

    coords = []
    for d in docs:
        coords.append(tuple(d['location']['coordinates']))

    gc = gj.GeometryCollection([gj.LineString(coords), gj.MultiPoint(coords)])

    start = arrow.get(docs[0]['timestamp']).to('America/Chicago').isoformat()
    end = arrow.get(docs[-1]['timestamp']).to('America/Chicago').isoformat()
    fname = 'GC_{vid}@{start}<-->{end}.geojson'.format(vid=vehicle_id, start=start, end=end)
    with open(fname, 'w') as f:
        gj.dump(gc, f)


def get_vehicle_positions(vehicle_id, num):
    return list(r.table('vehicle_position') \
                    .get_all(vehicle_id, index='vehicle_id') \
                    .order_by(r.desc('timestamp')) \
                    .limit(int(num)).run())


def get_vehicle_stop_times(vehicle_id, num=50, max_dist=100):
    positions = get_vehicle_positions(vehicle_id, num)
    res = [] # res = [(time_n, stop_a),(time_n-1, stop_b)...]

    # for each vehicle position VP, identify the closest stop
    for pos in positions:
        query = r.table('stops').get_nearest(pos['location'], index='location', max_results=1, max_dist=max_dist)
        closest_stops = query.run()
        if len(closest_stops) == 0:
            continue
        stop_id = closest_stops[0]['doc']['stop_id']

        # check if this stop has been added at a different time
        if res and res[-1][1] == stop_id:
            existing = arrow.get(res[-1][0])
            current = arrow.get(pos['timestamp'].isoformat())
            # if the bus was nearby at an earlier time, use the earlier time
            if current < existing:
                res[-1] = (pos['timestamp'].isoformat(), stop_id)
        else:
            res.append((pos['timestamp'].isoformat(), stop_id))

    return res


if __name__ == '__main__':
    # if len(sys.argv) < 3:
    #     print 'Usage: example.py <vehicle_id> <limit>'
    #     sys.exit(-1)

    conn = r.connect('localhost', 28015)
    conn.repl()
    conn.use('capmetro')
