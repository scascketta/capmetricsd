package main

import (
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
)

var (
	lastUpdated = map[string]time.Time{}

	normalDuration   = 30 * time.Second
	extendedDuration = 10 * time.Minute

	staleResponses      = map[string]int{}
	recentStaleResponse = map[string]bool{}
)

// FilterUpdatedVehicles returns the VehicleLocation structs whose timestamp has
// changed since the last update
func FilterUpdatedVehicles(vehicles []VehicleLocation) []VehicleLocation {
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
		staleResponses[route] = 1
	}
	if _, ok := recentStaleResponse[route]; !ok {
		recentStaleResponse[route] = false
	}
}

// LogVehicleLocations fetches vehicle locations from CapMetro and inserts new
// locations into the database. It also tracks stale responses to determine when
//  to sleep.
func LogVehicleLocations(session *r.Session) error {
	locationsByRoute, err := FetchVehicles()
	if err != nil {
		return err
	}

	for route, rl := range locationsByRoute {
		prepRoute(route)

		updated := FilterUpdatedVehicles(rl.Locations)
		if len(updated) == 0 {
			// increment retry count if fetch just before was also stale
			// only subsequent stale responses matter when determining how long to sleep
			if recentStaleResponse[route] {
				staleResponses[route]++
			}
			recentStaleResponse[route] = true
			dbglogger.Printf("No vehicles in response for route: %s.", route)
			continue
		}
		recentStaleResponse[route] = false

		for _, v := range updated {
			dbglogger.Printf("Vehicle %s updated at %s\n", v.VehicleID, v.Time.Format("2006-01-02T15:04:05-07:00"))
		}

		if len(updated) > 0 {
			_, err = r.Table("vehicle_position").Insert(r.Expr(updated)).Run(session)
			if err != nil {
				return err
			}
			dbglogger.Printf("Log %d vehicles, route %s.\n", len(updated), route)
		} else {
			dbglogger.Printf("No new vehicle positions to record for route %s.\n", route)
		}
	}
	return nil
}

// Check if the the routes are inactive
// There must have been MAX_RETRIES previous attempts to fetch data,
// and all attempts must have failed
func routesAreSleeping() bool {
	dbglogger.Println("staleResponses:", staleResponses)
	dbglogger.Println("recentStaleResponse:", recentStaleResponse)
	for _, retries := range staleResponses {
		if retries < config.MaxRetries {
			return false
		}
	}
	return true
}
