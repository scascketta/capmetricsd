package main

import (
	"fmt"
	r "github.com/scascketta/capmetro-log/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"log"
	"os"
	"sync"
	"time"
)

const MAX_RETRIES int = 5

var (
	dbglogger *log.Logger = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	errlogger *log.Logger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	session   *r.Session
	DB_NAME   string        = os.Getenv("DB_NAME")
	connOpts  r.ConnectOpts = r.ConnectOpts{Address: "localhost:28015", Database: DB_NAME}
	routes    []string      = []string{"803", "801", "550"}

	lastUpdated          map[string]time.Time = map[string]time.Time{}
	firstNewVehicleCheck bool                 = true
	nextNewVehicleCheck  time.Time            = time.Now()
	vehicleCheckInterval time.Duration        = (4 * 60 * 60) * (1000 * time.Millisecond)
	normalDuration       time.Duration        = (30) * (1000 * time.Millisecond)
	extendedDuration     time.Duration        = (10 * 60) * (1000 * time.Millisecond)
	sleepHistory         map[string]int       = map[string]int{}
)

func init() {
	if len(os.Getenv("DB_NAME")) == 0 {
		errlogger.Fatal("Missing envvar DB_NAME")
	}
}

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
	b, err := FetchVehicles(route)
	if err != nil {
		return err
	}

	vehicles, err := ParseVehiclesResponse(b)
	if err != nil {
		return err
	}
	if vehicles == nil {
		sleepHistory[route] += 1
		return fmt.Errorf("No vehicles in response for route: %s.\n", route)
	}

	updated := FilterUpdatedVehicles(vehicles)

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
	for _, retries := range sleepHistory {
		if retries < MAX_RETRIES {
			return false
		}
	}
	return true
}

func checkNewVehicles(session *r.Session) error {
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
	return nil
}

func main() {
	for _, route := range routes {
		sleepHistory[route] = 0
	}

	session, err := r.Connect(connOpts)
	if err != nil {
		errlogger.Fatal(err)
	}
	dbglogger.Printf("Established connection to RethinkDB server at %s.\n", connOpts.Address)

	var wg sync.WaitGroup

	for {
		// log new vehicle positions
		for _, route := range routes {
			wg.Add(1)
			go func(session *r.Session, route string) {
				err = LogVehiclePositions(session, route)
				if err != nil {
					errlogger.Println(err)
				}
				wg.Done()
			}(session, route)
		}

		// check for new vehicles if after next check time, add new ones
		// (added eventually, not necessarily as soon as a new vehicle appears in vehicle_positions table)
		if firstNewVehicleCheck || time.Now().After(nextNewVehicleCheck) {
			firstNewVehicleCheck = false
			wg.Add(1)
			go func() {
				err = checkNewVehicles(session)
				if err != nil {
					errlogger.Println(err)
				}
				wg.Done()
			}()
			nextNewVehicleCheck = time.Now().Add(vehicleCheckInterval)
			dbglogger.Println("Next check for new vehicles scheduled at:", nextNewVehicleCheck)
		}
		wg.Wait()

		// determine how long to sleep
		// if no vehicles were received from capmetro MAX_RETRIES in a row, sleep longer
		var duration time.Duration
		if routesAreSleeping() {
			for k, _ := range sleepHistory {
				sleepHistory[k] = 0
			}
			dbglogger.Println("Sleeping for extended duration!")
			duration = extendedDuration
		} else {
			dbglogger.Println("Sleeping for normal duration!")
			duration = normalDuration
		}

		time.Sleep(duration)
		dbglogger.Println("Wake up!")
	}
}
