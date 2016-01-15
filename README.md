# capmetricsd

A tool to archive GTFS-realtime data. For the time being, capmetricsd only archives [Vehicle Positions](https://developers.google.com/transit/gtfs-realtime/reference#VehiclePosition) from a GTFS-realtime feed.

# Install

1. [Install Go](https://golang.org/doc/install).
2. `go get github.com/scascketta/capmetricsd`
3. `go install github.com/scascketta/capmetricsd`

**NOTE:** Make sure the directory for binaries in your Go workspace (located at `$GOPATH`) has been added to your `PATH` like so:

`export PATH=$PATH:$GOPATH/bin`

# Usage

### Archiving Data

To start archiving data run:
```
capmetricsd start -t target-url --db db-path [--cronitor cronitor-url]
```

```
--target-url, -t 		URL to a GTFS-realtime Vehicle Positions feed
--db-path, --db 		Path to a BoltDB database (which will be created if it doesn't already exist).
--cronitor-url, --cron 	(OPTIONAL) URL to send requests to notify Cronitor (or comparable monitoring service).
```

This runs forever in the foreground. I recommend using some kind of process supervision service like Systemd, [runit](http://smarden.org/runit/), or [Supervisor](http://supervisord.org/) to keep it running.

**NOTE:** capmetricsd uses an embedded key/value store called [BoltDB](https://github.com/boltdb/bolt), which stores data as a single file on disk. A process using a BoltDB database obtains a file lock when it opens the file, so be aware that you must designate a different database for each process running capmetricsd.

### Retrieving Archived Data

You can access archived data as CSV data, specified with a time range in UNIX time.

```
capmetricsd get db dest min max
```

For example, to get archived data stored in a file called `capmetro.boltdb` from 1449813600 to 1449900000 and store the output in a file called `2015-12-11.csv`:

```
capmetricsd get capmetro.boltdb 2015-12-11.csv 1449813600 1449900000
```

# Internals

```
BUCKET (vehicle_locations)
    - BUCKET (trip_id_0)
        - timestamp_0 -> <data>
        - timestamp_1 -> <data>
        ...
        - timestamp_N -> <data>
    ...
    - BUCKET(trip_id_N)
```

Vehicle position data is stored in [nested buckets](https://github.com/boltdb/bolt/blob/f27abf2cc7fc695b13a06b0d6d7149125730b35b/README.md#nested-buckets) in a BoltDB database under the bucket [`vehicle_locations`](https://github.com/scascketta/capmetricsd/blob/05583538fdfac12c393ddcb7ee2250407842e43c/daemon/daemon.go#L14). The `vehicle_locations` bucket contains buckets named by [GTFS trip IDs](https://developers.google.com/transit/gtfs/reference#tripstxt). Each trip bucket contains all the vehicle position data for that trip, with the UNIX time for that position as the key. Keys in BoltDB are stored in byte-sorted order, so the vehicle position data in a trip bucket is sorted by time.

# Public Archived Data

The captured vehicle location data for Austin's transit agency (Capital Metro) is made available the next day on the [CapMetrics](https://github.com/scascketta/CapMetrics) repo.
