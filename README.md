# CapMetrics
This repo contains code related to collecting and analyzing vehicle location data provided by [Capital Metro](http://www.capmetro.org/).
# Available Data

The captured vehicle location data for each day is made available the next day on the `data` branch under `data/vehicle_locations` ([shortcut](https://github.com/scascketta/CapMetrics/tree/data/data)). The data is available as CSV files. Do whatever you like, but please credit back here if you make something public.

## Data Format
At the moment, only [vehicle positions](https://developers.google.com/transit/gtfs-realtime/reference#VehiclePosition) are recorded.

Note: Most of the optional fields of the VehiclePosition message in the GTFS-RT spec are **omitted** in CapMetro's implementation. Here's what their data looks like:

| field | description | GTFS-RT reference |
| --- | --- | --- |
| vehicle_id | ID of the transit vehicle | [VehicleDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#VehicleDescriptor) |
| speed | speed of the vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| lon | longitude of vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| lat | latitude of vehicle when position was recorded | [Position](https://developers.google.com/transit/gtfs-realtime/reference#Position) |
| route_id | ID of the route the vehicle is assigned to | [TripDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#TripDescriptor) |
| timestamp | Moment at which the vehicle's position was measured | [VehiclePosition](https://developers.google.com/transit/gtfs-realtime/reference#VehiclePosition) |
| trip_id | Refers to a trip from the GTFS feed | [TripDescriptor](https://developers.google.com/transit/gtfs-realtime/reference#TripDescriptor) |
| dist_traveled | The distance (in miles) traveled by the vehicle along the shape of the current trip. If the shape for the trip is not available, this will be set to -1. This metric is not provided by CapMetro, so I calculate it as best I can. | N/A |


# Code Layout

At the moment, there's the Go daemon which runs continuously on a set interval and sleeps longer when every route is inactive. CapMetro publishes a stream of real-time vehicle locations which the daemon captures and passes off to RethinkDB. The CapMetro specific code is located in `agency/capmetro`.  The `agency` package contains code for parsing and fetching data from transit agencies, though only CapMetro is supported for now. The `task` package contains the code for running functions in their own goroutine on a fixed and dynamic interval.

`bootstrap.py` contains code for bootstrapping a new RethinkDB database with routes and stops from CapMetro's GTFS data.

`reporting.py` contains code for generating some GeoJSON data ([example](https://gist.github.com/scascketta/3e93227da2558246f2e3)) from recorded vehicle locations, as well as some useful queries e.g. getting the historical travel time between two stops.
