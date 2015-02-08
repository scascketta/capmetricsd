package main

import (
	"fmt"
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

	emptyResponses      = map[string]int{}
	recentEmptyResponse = map[string]bool{}
)

// VehicleStopTime associates a vehicle being at a certain location at a specific time
type VehicleStopTime struct {
	VehicleID   string    `gorethink:"vehicle_id"`
	Route       string    `gorethink:"route"`
	TripID      string    `gorethink:"trip_id"`
	StopID      string    `gorethink:"stop_id"`
	Direction   string    `gorethink:"direction"`
	Time        time.Time `gorethink:"timestamp"`
	MaxDistance int       `gorethink:"max_distance"`
}

// FilterUpdatedVehicles returns the VehiclePosition structs whose timestamp has
// changed since the last update
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

// LogVehiclePositions fetches vehicle locations from CapMetro and inserts new
// locations into the database. It also tracks empty responses to determine when
//  to sleep.
func LogVehiclePositions(session *r.Session, route string) error {
	vehicles, err := FetchVehicles(route)
	if err != nil {
		return err
	}
	if vehicles == nil {
		// increment retry count if fetch just before was also empty
		// only subsequent empty responses matter when determining how long to sleep
		if recentEmptyResponse[route] {
			emptyResponses[route]++
		}
		recentEmptyResponse[route] = true
		dbglogger.Printf("No vehicles in response for route: %s.", route)
		return nil
	}
	recentEmptyResponse[route] = false

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

// MakeVehicleStopTimes finds the closest stop at a given time for each recorded vehicle position
// A vehicle stop time (or just stop time) is a struct that relates a vehicle to a stop at a specific time
func MakeVehicleStopTimes(session *r.Session) error {

	// Get all vehicles
	vehicles := []Vehicle{}
	cur, err := r.Db("capmetro").Table("vehicles").Run(session)
	if err != nil {
		return err
	}
	cur.All(&vehicles)
	if vehicles == nil {
		return fmt.Errorf("No vehicles available for making stop times.")
	}

	// get all recorded positions for each vehicle
	for _, vehicle := range vehicles {

		stopTimes := []VehicleStopTime{}
		// Not using []VehiclePosition because gorethink has trouble unmarshaling the Location field
		positions := []map[string]interface{}{}

		// get all vehicle_positions for vehicle_id after the vehicle was last analyzed
		betweenOpts := r.BetweenOpts{Index: "vehicle_timestamp"} // a compound index on a vehicle's id and timestamp
		orderByOpts := r.OrderByOpts{Index: "vehicle_timestamp"} // must use same index to chain the orderBy with secondary index
		lowerKey := r.Expr([]interface{}{vehicle.VehicleID, vehicle.LastAnalyzed})
		upperKey := r.Expr([]interface{}{vehicle.VehicleID, r.EpochTime(2000005200)})
		query := r.Db("capmetro").Table("vehicle_position")
		query = query.Between(lowerKey, upperKey, betweenOpts).OrderBy(r.Desc("vehicle_timestamp"), orderByOpts)

		cur, err := query.Run(session)
		if err != nil {
			errlogger.Println(err)
			continue
		}
		cur.All(&positions)
		if positions == nil {
			dbglogger.Printf("No positions available for vehicle %s after %s.\n", vehicle.VehicleID, vehicle.LastAnalyzed.Format("2006-01-02T15:04:05-07:00"))
			continue
		}

		dbglogger.Printf("Processing %d positions for vehicle %s after %s.\n", len(positions), vehicle.VehicleID, vehicle.LastAnalyzed.Format("2006-01-02T15:04:05-07:00"))
		for _, position := range positions {

			// find the closest stop within 100m for each position (if any)
			stops := []map[string]interface{}{}
			gnOpts := r.GetNearestOpts{Index: "location", MaxDist: config.MaxDistance, MaxResults: 1}
			query := r.Db("capmetro").Table("stops").GetNearest(position["location"], gnOpts)

			cur, err := query.Run(session)
			if err != nil {
				errlogger.Println(err)
				continue
			}
			cur.All(&stops)
			if len(stops) == 0 {
				continue
			}

			// the result of a GetNearest geospatial query contains the distance from the point specified (indexed by "dist")
			// and the document for the closest stop (indexed by "doc")
			stop := stops[0]["doc"].(map[string]interface{})
			stopTime := VehicleStopTime{
				VehicleID:   vehicle.VehicleID,
				Route:       position["route"].(string),
				TripID:      position["trip_id"].(string),
				StopID:      stop["stop_id"].(string),
				Time:        position["timestamp"].(time.Time),
				Direction:   position["direction"].(string),
				MaxDistance: config.MaxDistance,
			}

			if len(stopTimes) > 0 && stopTimes[len(stopTimes)-1].StopID == stopTime.StopID {
				// Replace most recent stopTime if current stopTime has earlier timestamp
				// We want to avoid duplicate stopTimes and in the case of duplicate, use the earlier stopTime
				if stopTime.Time.Before(stopTimes[len(stopTimes)-1].Time) {
					stopTimes[len(stopTimes)-1] = stopTime
				}
				continue
			} else {
				stopTimes = append(stopTimes, stopTime)
			}
			dbglogger.Printf("Added stopTime: stop=%s, time=%s.\n", stopTime.StopID, stopTime.Time.Format("2006-01-02T15:04:05-07:00"))
		}

		_, err = r.Db("capmetro").Table("vehicle_stop_times").Insert(r.Expr(stopTimes)).Run(session)
		if err != nil {
			errlogger.Println(err)
		}
		dbglogger.Printf("Added %d stop times for vehicle %s.\n", len(stopTimes), vehicle.VehicleID)
		vehicle.LastAnalyzed = time.Now()
		_, err = r.Db("capmetro").Table("vehicles").Get(vehicle.ID).Update(r.Expr(vehicle)).RunWrite(session)
		if err != nil {
			return err
		}
	}
	return nil
}
