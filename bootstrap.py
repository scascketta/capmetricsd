#!/usr/bin/env python
import os

import requests
import rethinkdb as r

DB_NAME = 'capmetro'
BASE_URL = 'https://raw.githubusercontent.com/luqmaan/Instabus/39d10466bb47a6c95ce7e1c527724b850f0284aa'
ROUTES_URL = BASE_URL + '/data/routes.json'


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

    # Make direction field consistent
    for route in routes_data:
        for d in route['directions']:
            d['direction'] = d['headsign'][0]

    r.table('routes').insert(routes_data).run(conn)

    return routes_data


def setup_stops(conn, routes_data):
    table_name = 'stops'
    _create_table_if_not_exists(conn, table_name)
    _create_index_if_not_exists(conn, 'stops', 'location', ['location', r.row['location']], {'geo':True})

    all_stops = []
    for route in routes_data:
        for direction in route['directions']:
            # route_direction = str(route['route_id']) + '_' + str(direction['direction_id'])
            stop_url = '{}/data/stops_{}_{}.json'.format(BASE_URL, route['route_id'], direction['direction_id'])
            stops_data = requests.get(stop_url).json()

            for stop in stops_data:
                # add location as rethink geo point
                stop['location'] = r.point(stop['stop_lon'], stop['stop_lat'])
                # too similar to location.type attr in rethinkdb
                del stop['location_type']

            all_stops.extend(stops_data)

    r.table(table_name).insert(all_stops).run(conn)


def setup_shapes(conn, routes_data):
    table_name = 'shapes'
    _create_table_if_not_exists(conn, table_name)
    _create_index_if_not_exists(conn, 'shapes', 'location', ['location', r.row['location']], {'geo':True})

    all_shapes = []
    for route in routes_data:
        for direction in route['directions']:
            stop_url = '{}/data/shapes_{}_{}.json'.format(BASE_URL, route['route_id'], direction['direction_id'])
            shapes_data = requests.get(stop_url).json()

            for shape in shapes_data:
                # add location as rethink geo point
                shape['location'] = r.point(shape['shape_pt_lon'], shape['shape_pt_lat'])

            all_shapes.extend(shapes_data)

    r.table(table_name).insert(all_shapes).run(conn)


def setup_vehicle_positions(conn):
    table_name = 'vehicle_position'
    _create_table_if_not_exists(conn, table_name)

    _create_index_if_not_exists(conn, 'vehicle_position', 'location', ['location', r.row['location']], {'geo':True})
    _create_index_if_not_exists(conn, 'vehicle_position', 'timestamp', ['timestamp', r.row['timestamp']])
    _create_index_if_not_exists(conn, 'vehicle_position', 'vehicle_timestamp', ['vehicle_timestamp', [r.row['vehicle_id'], r.row['timestamp']]])
    _create_index_if_not_exists(conn, 'vehicle_position', 'route_timestamp', ['route_timestamp', [r.row['route_id'], r.row['timestamp']]])


def setup_vehicle_stop_times(conn):
    table_name = 'vehicle_stop_times'
    _create_table_if_not_exists(conn, table_name)

    _create_index_if_not_exists(conn, 'vehicle_stop_times', 'stop_id', ['stop_id', r.row['stop_id']])
    _create_index_if_not_exists(conn, 'vehicle_stop_times', 'timestamp', ['timestamp', r.row['timestamp']])


def setup_vehicles(conn):
    table_name = 'vehicles'
    _create_table_if_not_exists(conn, table_name)
    _create_index_if_not_exists(conn, 'vehicles', 'vehicle_id', ['vehicle_id', r.row['vehicle_id']])


def _create_index_if_not_exists(conn, table, index, args, kwargs=dict()):
        if index not in r.table(table).index_list().run(conn):
            r.table(table).index_create(*args, **kwargs).run(conn)

def setup_indexes(conn):
    _create_index_if_not_exists(conn, 'vehicle_position', 'location', ['location', r.row['location']], {'geo':True})
    _create_index_if_not_exists(conn, 'vehicle_position', 'timestamp', ['timestamp', r.row['timestamp']])
    _create_index_if_not_exists(conn, 'vehicle_position', 'vehicle_timestamp', ['vehicle_timestamp', [r.row['vehicle_id'], r.row['timestamp']]])
    _create_index_if_not_exists(conn, 'vehicle_position', 'route_timestamp', ['route_timestamp', [r.row['route_id'], r.row['timestamp']]])


if __name__ == '__main__':
    host_addr = os.environ.get('CMDATA_DBADDR') or 'localhost'
    conn = r.connect(host_addr, 28015)
    conn.use(DB_NAME)

    print 'Bootstrapping the {} database.'.format(DB_NAME)
    if DB_NAME not in r.db_list().run(conn):
        r.db_create(DB_NAME).run(conn)

    routes_data = setup_routes(conn)
    setup_stops(conn, routes_data)
    setup_shapes(conn, routes_data)
    setup_vehicles(conn)
    setup_vehicle_positions(conn)
    setup_vehicle_stop_times(conn)
    setup_indexes(conn)
    conn.close()
