package main

import (
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
)

var (
	lastUpdated = map[string]time.Time{}

	firstNewVehicleCheck = true
	nextNewVehicleCheck  = time.Now()
	vehicleCheckInterval = (4 * 60 * 60) * (1000 * time.Millisecond)

	normalDuration   = (30) * (1000 * time.Millisecond)
	extendedDuration = (10 * 60) * (1000 * time.Millisecond)

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
		if !updateTime.Equal(v.Time) {
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

// Check if any new vehicles appear in recorded vehicle positions, add them to vehicles table
func checkNewVehicles(session *r.Session) error {
	newVehicles := 0
	dbglogger.Println("Check for new vehicles.")
	vehicles := []map[string]string{}
	cur, err := r.Table("vehicle_position").Pluck("vehicle_id", "route", "route_id", "trip_id").Distinct().Run(session)
	if err != nil {
		return err
	}
	cur.All(&vehicles)

	for _, data := range vehicles {
		id := data["vehicle_id"]
		stream := r.Table("vehicles").Pluck("vehicle_id")
		expr := r.Expr(map[string]string{"vehicle_id": data["vehicle_id"]})
		cur, err = stream.Contains(expr).Run(session)
		if err != nil {
			return err
		}
		var res bool

		cur.Next(&res)
		if !res {
			newVehicles++
			dbglogger.Printf("Adding new vehicle %s to vehicles table.\n", id)
			vehicle := Vehicle{
				VehicleID:    data["vehicle_id"],
				Route:        data["route"],
				RouteID:      data["route_id"],
				TripID:       data["trip_id"],
				LastAnalyzed: time.Now(),
			}
			_, err := r.Table("vehicles").Insert(r.Expr(vehicle)).Run(session)
			if err != nil {
				return err
			}
		}
	}

	dbglogger.Printf("Inserted %d new vehicles.\n", newVehicles)
	return nil
}
