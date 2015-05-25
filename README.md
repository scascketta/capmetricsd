# capmetricsd
This repo contains code related to collecting and analyzing vehicle location data provided by [Capital Metro](http://www.capmetro.org/).
# Available Data

The captured vehicle location data for each day is made available the next day on the [CapMetrics](https://github.com/scascketta/CapMetrics) repo.

# Code Layout

At the moment, there's the Go daemon which runs continuously on a set interval and sleeps longer when every route is inactive. CapMetro publishes a stream of real-time vehicle locations which the daemon captures and passes off to RethinkDB. The CapMetro specific code is located in `agency/capmetro`.  The `agency` package contains code for parsing and fetching data from transit agencies, though only CapMetro is supported for now. The `task` package contains the code for running functions in their own goroutine on a fixed and dynamic interval.

`bootstrap.py` contains code for bootstrapping a new RethinkDB database with routes and stops from CapMetro's GTFS data.

`reporting.py` contains code for generating some GeoJSON data ([example](https://gist.github.com/scascketta/3e93227da2558246f2e3)) from recorded vehicle locations, as well as some useful queries e.g. getting the historical travel time between two stops.
