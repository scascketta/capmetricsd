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


def get_travel_time(stop_start, stop_end, route, direction, time=arrow.now(), window=45):
    # get all vehicles within [time-window, time] that have visited stop_start
    vehicles = _get_vehicles_by_stop(stop_start, route, direction, time, window)

    # find the range of times for the first vehicle that has traveled between stop_start and stop_end within [now-window, now]
    stop_times = None
    for vehicle in vehicles:
        stop_times = _get_stop_times_by_vehicle(vehicle['vehicle_id'], stop_start, stop_end, route, direction, time, window)
        if len(stop_times) == 2:
            break

    if stop_times is None or len(stop_times) != 2:
        return None

    return stop_times[1]['timestamp'] - stop_times[0]['timestamp']


def _get_vehicles_by_stop(stop, route, direction, time=arrow.now(), window=45, index='route_direction_stop_time'):
    lower_time = r.epoch_time(time.replace(minutes=-window).timestamp)
    upper_time = r.epoch_time(time.timestamp)

    lower_key = [route, direction, stop, lower_time]
    upper_key = [route, direction, stop, upper_time]

    query = r.table('vehicle_stop_times') \
                .between(lower_key, upper_key, index=index) \
                .order_by(index=r.desc(index))

    return list(query.run())


def _get_stop_times_by_vehicle(vehicle, stop_start, stop_end, route, direction, time=arrow.now(), window=45, index='route_direction_vehicle_time'):
    lower_key = [route, direction, vehicle, r.epoch_time(time.replace(minutes=-window).timestamp)]
    upper_key = [route, direction, vehicle, r.epoch_time(time.timestamp)]

    query = r.table('vehicle_stop_times') \
                .between(lower_key, upper_key, index=index) \
                .order_by(index=r.asc(index)) \
                .filter((r.row['stop_id'] == stop_start) | (r.row['stop_id'] == stop_end))

    return list(query.run())


def get_vehicle_positions(vehicle_id, num):
    return list(r.table('vehicle_position') \
                    .get_all(vehicle_id, index='vehicle_id') \
                    .order_by(r.desc('timestamp')) \
                    .limit(int(num)).run())


if __name__ == '__main__':
    import os
    host_addr = os.environ.get('CMDATA_DBADDR') or 'localhost'
    conn = r.connect(host_addr, 28015)
    conn.repl()
    conn.use('capmetro')
