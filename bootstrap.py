#!/usr/bin/env python
import os

import requests
import rethinkdb as r

DB_NAME = 'test'
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


def _table_exists(conn, table):
    return table in r.table_list().run(conn)


def _create_table_if_not_exists(conn, table):
    print 'Bootstrapping the {} table.'.format(table)

    if not _table_exists(conn, table):
        r.db(DB_NAME).table_create(table).run(conn)
    else:
        print 'Table {} already exists, skipping'.format(table)


def setup_routes(conn, url=ROUTES_URL):
    table_name = 'routes'
    _create_table_if_not_exists(conn, table_name)

    routes_data = requests.get(url).json()

    # Add consistent direction field
    for route in routes_data:
        for d in route['directions']:
            dir_id = d['direction_id']
            d['direction'] = 'N' if dir_id == 0 else 'S'
            if route['route_id'] in BACKWARDS_ROUTES:
                d['direction'] = 'N' if dir_id == 1 else 'S'

    r.table('routes').insert(routes_data).run(conn)

    return routes_data


def setup_stops(conn, routes_data):
    table_name = 'stops'
    _create_table_if_not_exists(conn, table_name)
    r.table(table_name).index_create('location', r.row['location'], geo=True).run(conn)

    all_stops = []
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

            all_stops.extend(stops_data)

    r.table(table_name).insert(all_stops).run(conn)


def setup_vehicle_positions(conn):
    table_name = 'vehicle_position'
    _create_table_if_not_exists(conn, table_name)

    r.table(table_name).index_create('location', r.row['location'], geo=True).run(conn)
    r.table(table_name).index_create('timestamp', r.row['timestamp']).run(conn)
    r.table(table_name).index_create('vehicle_timestamp', [r.row['vehicle_id'], r.row['timestamp']]).run(conn)


def setup_vehicle_stop_times(conn):
    table_name = 'vehicle_stop_times'
    _create_table_if_not_exists(conn, table_name)

    r.table(table_name).index_create('stop_id', r.row['stop_id']).run(conn)
    r.table(table_name).index_create('timestamp', r.row['timestamp']).run(conn)


def setup_vehicles(conn):
    table_name = 'vehicles'
    _create_table_if_not_exists(conn, table_name)

    r.table(table_name).index_create('vehicle_id', r.row['vehicle_id']).run(conn)


if __name__ == '__main__':
    host_addr = os.environ.get('CMDATA_DBADDR') or 'localhost'
    conn = r.connect(host_addr, 28015)
    conn.use(DB_NAME)

    print 'Bootstrapping the {} database.'.format(DB_NAME)
    if DB_NAME not in r.db_list().run(conn):
        r.db_create(DB_NAME).run(conn)

    routes_data = setup_routes(conn)
    setup_stops(conn, routes_data)
    setup_vehicles(conn)
    setup_vehicle_positions(conn)
    setup_vehicle_stop_times(conn)
    conn.close()
