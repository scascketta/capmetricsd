package main

import (
	"fmt"
	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"time"
)

var (
	lastUpdated map[string]time.Time = map[string]time.Time{}

	firstNewVehicleCheck bool          = true
	nextNewVehicleCheck  time.Time     = time.Now()
	vehicleCheckInterval time.Duration = (4 * 60 * 60) * (1000 * time.Millisecond)

	normalDuration   time.Duration = (30) * (1000 * time.Millisecond)
	extendedDuration time.Duration = (10 * 60) * (1000 * time.Millisecond)

	emptyResponses      map[string]int  = map[string]int{}
	recentEmptyResponse map[string]bool = map[string]bool{}
)

func FilterUpdatedVehicles(vehicles []VehiclePosition) []VehiclePosition {
	updated := []VehiclePosition{}
	for _, v := range vehicles {
		updateTime, _ := lastUpdated[v.VehicleID]
		lastUpdated[v.VehicleID] = v.Time
		if !updateTime.Equal(v.Time) {
			updated = append(updated, v)
		}
	}
	return updated
}

func LogVehiclePositions(session *r.Session, route string) error {
	vehicles, err := FetchVehicles(route)
	if err != nil {
		return err
	}
	if vehicles == nil {
		// increment retry count if fetch just before was also empty
		// only subsequent empty responses matter when determining how long to sleep
		if recentEmptyResponse[route] {
			emptyResponses[route] += 1
		}
		recentEmptyResponse[route] = true
		return fmt.Errorf("No vehicles in response for route: %s.", route)
	} else {
		recentEmptyResponse[route] = false
	}

	updated := FilterUpdatedVehicles(vehicles)

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
	return nil
}

// Check if the the routes are inactive
// There must have been MAX_RETRIES previous attempts to fetch data,
// and all attempts must have failed
func routesAreSleeping() bool {
	dbglogger.Println("emptyResponses:", emptyResponses)
	dbglogger.Println("recentEmptyResponse:", recentEmptyResponse)
	for _, retries := range emptyResponses {
		if retries < MAX_RETRIES {
			return false
		}
	}
	return true
}

func checkNewVehicles(session *r.Session) error {
	new_vehicles := 0
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
		query_expr := r.Expr(map[string]string{"vehicle_id": data["vehicle_id"]})
		cur, err = stream.Contains(query_expr).Run(session)
		if err != nil {
			return err
		}
		var res bool

		cur.Next(&res)
		if !res {
			new_vehicles += 1
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

	dbglogger.Printf("Inserted %d new vehicles.\n", new_vehicles)
	return nil
}
