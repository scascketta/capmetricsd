package capmetro

import (
	"log"
	"os"
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
)

var (
	dlog             = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog             = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	normalDuration   = 30 * time.Second
	extendedDuration = 10 * time.Minute
	lastUpdated      = map[string]time.Time{}
	staleResponses   = map[string]int{}
)

// filterUpdatedVehicles returns the VehicleLocation structs whose timestamp has
// changed since the last update
func filterUpdatedVehicles(vehicles []VehicleLocation) []VehicleLocation {
	updated := []VehicleLocation{}
	for _, v := range vehicles {
		updateTime, _ := lastUpdated[v.VehicleID]
		lastUpdated[v.VehicleID] = v.Time
		threshold := time.Now().Add(time.Minute)
		// reject location updates if timestamp hasn't changed or is more than 1 minute in the future
		if !updateTime.Equal(v.Time) && v.Time.Before(threshold) {
			updated = append(updated, v)
		}
	}
	return updated
}

func prepRoute(route string) {
	if _, ok := staleResponses[route]; !ok {
		staleResponses[route] = 0
	}
}

func LogVehicleLocations(setupConn func() *r.Session) func() error {
	return func() error {
		session := setupConn()
		err := logVehicleLocations(session)
		session.Close()
		return err
	}
}

// LogVehicleLocations fetches vehicle locations from CapMetro and inserts new
// locations into the database. It also tracks stale responses to determine when
//  to sleep.
func logVehicleLocations(session *r.Session) error {
	locationsByRoute, err := FetchVehicles()
	if err != nil {
		return err
	}

	for route, rl := range locationsByRoute {
		prepRoute(route)

		updated := filterUpdatedVehicles(rl.Locations)
		if len(updated) == 0 {
			// increment retry count if fetch just before was also stale
			// only subsequent stale responses matter when determining how long to sleep
			staleResponses[route]++
			dlog.Printf("No vehicles in response for route: %s.", route)
			continue
		} else {
			staleResponses[route] = 0
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
func routesAreSleeping(maxRetries int) bool {
	dlog.Println("staleResponses:", staleResponses)
	for _, retries := range staleResponses {
		if retries < maxRetries {
			return false
		}
	}
	return true
}

func UpdateInterval(maxRetries int) func() (bool, time.Duration) {
	return func() (bool, time.Duration) {
		if routesAreSleeping(maxRetries) {
			for k := range staleResponses {
				staleResponses[k] = 0
			}
			dlog.Println("Sleeping for extended duration!")
			return true, extendedDuration
		} else {
			dlog.Println("Sleeping for normal duration!")
			return false, normalDuration
		}
	}
}
