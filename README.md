# CapMetrics
This repo contains code related to collecting and analyzing vehicle location data provided by [Capital Metro](http://www.capmetro.org/), primarily for use in [Instabus](https://github.com/luqmaan/Instabus) (specifically [real-time arrival info](https://github.com/luqmaan/Instabus/issues/184)).

At the moment, there's the Go daemon (composed of `main.go`, `fetch.go`, and `capmetro.go`) which runs continuously. CapMetro publishes a stream of real-time vehicle locations which the daemon consumes. It adds any new vehicles from the stream, and also assigns stop IDs to vehicle locations so we can identify what time a vehicle was at a stop. 

`bootstrap.py` contains code for bootstrapping a new RethinkDB database with routes and stops from CapMetro's GTFS data.

`reporting.py` contains code for generating some GeoJSON data ([example](https://gist.github.com/scascketta/3e93227da2558246f2e3)) from recorded vehicle locations, as well as some useful queries e.g. getting the historical travel time between two stops.
