package capmetro

import (
	"log"
	"os"
	"time"

	r "github.com/scascketta/CapMetrics/Godeps/_workspace/src/github.com/dancannon/gorethink"
)

const (
	NormalDuration   = 30 * time.Second
	ExtendedDuration = 10 * time.Minute
)

var (
	dlog = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

type FetchHistory struct {
	LastUpdated    map[string]time.Time
	StaleResponses map[string]int
}

func NewFetchHistory() *FetchHistory {
	fh := new(FetchHistory)
	fh.LastUpdated = make(map[string]time.Time)
	fh.StaleResponses = make(map[string]int)
	return fh
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
		}
	}
	return updated
}

func prepRoute(route string, fh *FetchHistory) {
	if _, ok := fh.StaleResponses[route]; !ok {
		fh.StaleResponses[route] = 0
	}
}

func LogVehicleLocations(setupConn func() *r.Session, fh *FetchHistory) func() error {
	return func() error {
		session := setupConn()
		err := logVehicleLocations(session, fh)
		session.Close()
		return err
	}
}

// LogVehicleLocations fetches vehicle locations from CapMetro and inserts new
// locations into the database. It also tracks stale responses to determine when
//  to sleep.
func logVehicleLocations(session *r.Session, fh *FetchHistory) error {
	locationsByRoute, err := FetchVehicles()
	if err != nil {
		return err
	}

	for route, rl := range locationsByRoute {
		prepRoute(route, fh)

		updated := filterUpdatedVehicles(rl.Locations, fh)
		if len(updated) == 0 {
			// increment retry count if fetch just before was also stale
			// only subsequent stale responses matter when determining how long to sleep
			fh.StaleResponses[route]++
			dlog.Printf("No vehicles in response for route: %s.", route)
			continue
		} else {
			fh.StaleResponses[route] = 0
		}

		for _, v := range updated {
			dlog.Printf("Vehicle %s updated at %s\n", v.VehicleID, v.Time.Format("2006-01-02T15:04:05-07:00"))
		}

		if len(updated) > 0 {
			_, err = r.Table("vehicle_position").Insert(r.Expr(updated)).Run(session)
			if err != nil {
				return err
			}
			dlog.Printf("Log %d vehicles, route %s.\n", len(updated), route)
		} else {
			dlog.Printf("No new vehicle positions to record for route %s.\n", route)
		}
	}
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

func UpdateInterval(maxRetries int, fh *FetchHistory) func() (bool, time.Duration) {
	return func() (bool, time.Duration) {
		if routesAreSleeping(maxRetries, fh) {
			for k := range fh.StaleResponses {
				fh.StaleResponses[k] = 0
			}
			dlog.Println("Sleeping for extended duration!")
			return true, ExtendedDuration
		} else {
			dlog.Println("Sleeping for normal duration!")
			return false, NormalDuration
		}
	}
}
