package capmetro

import (
	"encoding/json"
	"fmt"
	"github.com/scascketta/CapMetrics/Godeps/_workspace/src/github.com/boltdb/bolt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	NormalDuration   = 30 * time.Second // Duration to wait between fetching locations when at least one route is active
	ExtendedDuration = 10 * time.Minute // Duration to wait between fetching locations when all routes are inactive
)

var (
	dlog = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

// FetchHistory contains the history of vehicle location fetches, including when a vehicle was last updated,
// and how many responses were "stale" for each route.
type FetchHistory struct {
	LastUpdated    map[string]time.Time
	StaleResponses map[string]int
}

func NewFetchHistory() *FetchHistory {
	return &FetchHistory{make(map[string]time.Time), make(map[string]int)}
}

// filterUpdatedVehicles returns the VehicleLocation structs whose timestamp has
// changed since the last update
func filterUpdatedVehicles(vehicles []VehicleLocation, fh *FetchHistory) []VehicleLocation {
	updated := []VehicleLocation{}
	for _, v := range vehicles {
		updateTime, _ := fh.LastUpdated[v.VehicleID]
		fh.LastUpdated[v.VehicleID] = v.Time
		threshold := time.Now().Add(time.Minute)
		// reject location updates if timestamp hasn't changed or is more than 1 minute in the future
		if !updateTime.Equal(v.Time) && v.Time.Before(threshold) {
			updated = append(updated, v)
			// dlog.Printf("Vehicle %s updated at %s\n", v.VehicleID, v.Time.Format("2006-01-02T15:04:05-07:00"))
		}
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

				var wg sync.WaitGroup
				for _, location := range updated {
					wg.Add(1)

					go func(location VehicleLocation) {

						err := db.Batch(func(tx *bolt.Tx) error {
							bucket, err := tx.CreateBucketIfNotExists([]byte("vehicle_locations"))
							if err != nil {
								return err
							}

							key := fmt.Sprintf("%s:%s", location.Time.Format("2006-01-02T15:04:05-07:00"), location.TripID)
							jsonVal, _ := json.Marshal(location)
							err = bucket.Put([]byte(key), jsonVal)
							return err
						})

						if err != nil {
							// let goroutines fail without affecting others
							elog.Println(err)
						}
						wg.Done()
					}(location)

				}
				wg.Wait()

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
