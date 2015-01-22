#!/usr/bin/env python
import os

import requests
import rethinkdb as r

BASE_URL = 'https://raw.githubusercontent.com/luqmaan/MetroRappid/b6b38d242bf8d248f603ea7e84af9a9110d77b18'
ROUTES_URL = BASE_URL + '/data/routes.json'
STOP_URLS = {
    '801_0': BASE_URL + '/data/stops_801_0.json',
    '801_1': BASE_URL + '/data/stops_801_1.json',
    '803_0': BASE_URL + '/data/stops_803_0.json',
    '803_1': BASE_URL + '/data/stops_803_1.json',
    '550_0': BASE_URL + '/data/stops_550_0.json',
    '550_1': BASE_URL + '/data/stops_550_1.json'
}
BACKWARDS_ROUTES = [550]


def fetch_routes(conn, url=ROUTES_URL):
    routes_data = requests.get(url).json()

    print 'Creating table for routes'
    r.table_create('routes').run(conn)

    # Add consistent direction field
    for route in routes_data:
        for d in route['directions']:
            dir_id = d['direction_id']
            d['direction'] = 'N' if dir_id == 0 else 'S'
            if route['route_id'] in BACKWARDS_ROUTES:
                d['direction'] = 'N' if dir_id == 1 else 'S'

    print 'Inserting routes into routes table'
    r.table('routes').insert(routes_data).run(conn)

    return routes_data


def fetch_stops(conn, routes_data):
    print 'Creating tables for stops'

    # fetch and clean stops data
    for route in routes_data:
        for direction in route['directions']:
            route_direction = str(route['route_id']) + '_' + str(direction['direction_id'])
            stops_data = requests.get(STOP_URLS[route_direction]).json()

            for stop in stops_data:
                # Add consistent direction field
                dir_id = stop['direction_id']
                stop['direction'] = 'N' if dir_id == 0 else 'S'
                if stop['route_id'] in BACKWARDS_ROUTES:
                    stop['direction'] = 'N' if dir_id == 1 else 'S'

                # add location as geojson
                stop['location'] = r.point(stop['stop_lon'], stop['stop_lat'])

                # too similar to location.type attr in rethinkdb
                del stop['location_type']

            stops_name = 'stops_' + route_direction
            print 'Creating stops table:', stops_name
            r.table_create(stops_name).run(conn)
            r.table(stops_name).insert(stops_data).run(conn)


if __name__ == '__main__':
    conn = r.connect('localhost', 28015)
    r.db_create('capmetro').run(conn)
    conn.use('capmetro')
    routes_data = fetch_routes(conn)
    fetch_stops(conn, routes_data)
