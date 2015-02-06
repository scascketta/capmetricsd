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


# get_travel_time('5863', '5867', '801', 'S')
def get_travel_time(stop_start, stop_end, route, direction, timestamp=arrow.now(), window=30):
    # get vehicles traveling between stop_start, stop_end
    lower_time = r.epoch_time(timestamp.replace(minutes=-window).timestamp)
    upper_time = r.epoch_time(timestamp.timestamp)
    lower_key = [route, direction, stop_start, lower_time]
    upper_key = [route, direction, stop_start, upper_time]
    query = r.table('vehicle_stop_times') \
                .between(lower_key, upper_key, index='route_direction_stop_time') \
                .order_by(index=r.desc('route_direction_stop_time'))
    vehicles = list(query.run())
    print '# vehicles:', len(vehicles)
    ind = 0
    stop_times = _get_stop_times_for_vehicle(vehicles[ind]['vehicle_id'], stop_start, stop_end, route, direction, timestamp=timestamp, window=window)
    print 'stop_times:', stop_times
    while len(stop_times) < 2 and ind < len(vehicles) - 1:
        ind += 1
        print ind
        stop_times = _get_stop_times_for_vehicle(vehicles[ind]['vehicle_id'], stop_start, stop_end, route, direction, timestamp=timestamp, window=window)
    return stop_times


# timestamp = arrow.get('2015-02-05T22:00:00Z')
# _get_stop_times_for_vehicle('5022', '5863', '5867', '801', 'S')
def _get_stop_times_for_vehicle(vehicle, stop_start, stop_end, route, direction, index='route_direction_vehicle_time', timestamp=arrow.now(), window=30):
    lower_key = [route, direction, vehicle, r.epoch_time(timestamp.timestamp)]
    upper_key = [route, direction, vehicle, r.epoch_time(timestamp.replace(minutes=+window).timestamp)]
    query = r.table('vehicle_stop_times') \
                .between(lower_key, upper_key, index=index) \
                .order_by(index=r.asc(index)) \
                .filter((r.row['stop_id'] == stop_start) | (r.row['stop_id'] == stop_end))

    return list(query.run())


def _get_stops_for_route_direction(route, direction, index='route_direction', ascending=True):
    right_key = chr(ord(direction) + 1)
    order = r.asc('stop_sequence') if ascending else r.desc('stop_sequence')

    query = r.table('stops').between([route, direction], [route, right_key], index=index)
    query = query.order_by(order)
    return list(query.run())


if __name__ == '__main__':
    # if len(sys.argv) < 3:
    #     print 'Usage: example.py <vehicle_id> <limit>'
    #     sys.exit(-1)

    conn = r.connect('104.131.115.42', 28015)
    conn.repl()
    conn.use('capmetro')
    timestamp = arrow.get('2015-02-05T22:00:00Z')
    times = get_travel_time('5863', '5867', '801', 'S', timestamp=timestamp)

