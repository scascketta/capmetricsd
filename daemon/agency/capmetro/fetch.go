package capmetro

import (
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/daemon/agency"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	NormalDuration   = 30 * time.Second // Duration to wait between fetching locations when at least one route is active
	ExtendedDuration = 10 * time.Minute // Duration to wait between fetching locations when all routes are inactive
	UnixFormat       = "1136214245"
	BucketName       = "vehicle_locations"
)

var (
	dlog = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

// FetchHistory contains the history of vehicle location fetches, including when a vehicle was last updated,
// and how many responses were "stale" for each route.
type FetchHistory struct {
	UpdateHistory  map[string]time.Time
	StaleResponses map[string]int
}

func NewFetchHistory() *FetchHistory {
	return &FetchHistory{make(map[string]time.Time), make(map[string]int)}
}

// filterUpdatedVehicles returns the VehicleLocation structs whose timestamp has
// changed since the last update
func filterUpdatedVehicles(locations []agency.VehicleLocation, fh *FetchHistory) []agency.VehicleLocation {
	updated := []agency.VehicleLocation{}

	for _, loc := range locations {
		vehicleID := loc.GetVehicleId()
		timestamp := time.Unix(loc.GetTimestamp(), 0)
		lastUpdate, _ := fh.UpdateHistory[vehicleID]

		fh.UpdateHistory[vehicleID] = timestamp

		// reject location updates if timestamp hasn't changed or is more than 1 minute in the future
		threshold := time.Now().Add(time.Minute)
		if lastUpdate.Equal(timestamp) && timestamp.After(threshold) {
			continue
		}

		if len(loc.GetTripId()) == 0 {
			continue
		}

		updated = append(updated, loc)
	}
	return updated
}

func prepRoute(route string, fh *FetchHistory) {
	if _, ok := fh.StaleResponses[route]; !ok {
		fh.StaleResponses[route] = 0
	}
}

// LogVehicleLocations calls setupConn for a *gorethink.Session to pass to logVehicleLocations and closes it afterwards.
func LogVehicleLocations(setupConn func() *bolt.DB, fh *FetchHistory) func() error {
	return func() error {
		db := setupConn()
		defer db.Close()
		err := logVehicleLocations(db, fh)
		return err
	}
}

func storeLocation(location agency.VehicleLocation, db *bolt.DB) {
	err := db.Batch(func(tx *bolt.Tx) error {
		topBucket, err := tx.CreateBucketIfNotExists([]byte(BucketName))
		if err != nil {
			return err
		}
		tripBucket, err := topBucket.CreateBucketIfNotExists([]byte(location.GetTripId()))
		if err != nil {
			return err
		}

		data, err := proto.Marshal(&location)
		if err != nil {
			return err
		}

		// key is POSIX time
		ts := location.GetTimestamp()
		key := strconv.Itoa(int(ts))

		err = tripBucket.Put([]byte(key), data)
		if err != nil {
			elog.Fatal(err)
		}
		return err
	})

	if err != nil {
		elog.Println(err)
	}
}

// logVehicleLocations fetches vehicle locations from CapMetro and inserts new
// locations into the database. It also tracks stale responses to determine when
//  to sleep.
func logVehicleLocations(db *bolt.DB, fh *FetchHistory) error {
	locationsByRoute, err := FetchVehicles()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for route, rl := range locationsByRoute {
		wg.Add(1)
		go func(route string, rl RouteLocations) {

			prepRoute(route, fh)

			updated := filterUpdatedVehicles(rl.Locations, fh)

			if len(updated) == 0 {
				fh.StaleResponses[route]++
				dlog.Printf("No vehicles in response for route: %s.", route)
			} else {
				fh.StaleResponses[route] = 0

				for _, location := range updated {
					storeLocation(location, db)
				}

				// FIXME: Do this once for every slice of updated vehicles
				dlog.Printf("Log %d vehicles, route %s.\n", len(updated), route)
			}
			wg.Done()
		}(route, rl)
	}
	wg.Wait()
	return nil
}

// Check if the the routes are inactive
// There must have been MAX_RETRIES previous attempts to fetch data,
// and all attempts must have failed
func routesAreSleeping(maxRetries int, fh *FetchHistory) bool {
	dlog.Println("staleResponses:", fh.StaleResponses)
	for _, retries := range fh.StaleResponses {
		if retries < maxRetries {
			return false
		}
	}
	return true
}

// UpdateInterval changes the interval between fetches if MAX_RETRIES responses have been stale for every route
func UpdateInterval(maxRetries int, fh *FetchHistory) func() time.Duration {
	return func() time.Duration {
		if routesAreSleeping(maxRetries, fh) {
			for k := range fh.StaleResponses {
				fh.StaleResponses[k] = 0
			}
			return ExtendedDuration
		}
		return NormalDuration
	}
}
