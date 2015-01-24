package main

import (
	r "github.com/scascketta/capmetro-log/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"log"
	"os"
	"sync"
	"time"
)

const MAX_RETRIES int = 5

var (
	errlogger *log.Logger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	session   *r.Session
	DB_NAME   string        = os.Getenv("DB_NAME")
	connOpts  r.ConnectOpts = r.ConnectOpts{Address: "localhost:28015", Database: DB_NAME}
	routes    []string      = []string{"803", "801", "550"}

	lastUpdated      map[string]time.Time = map[string]time.Time{}
	normalDuration   time.Duration        = 30 * (1000 * time.Millisecond)
	extendedDuration time.Duration        = (10 * 60) * (1000 * time.Millisecond)
	sleepHistory     map[string]int       = map[string]int{}
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

func LogVehiclePositions(session *r.Session, route string) {
	b, err := FetchVehicles(route)
	if err != nil {
		errlogger.Println(err)
		return
	}

	vehicles, err := ParseVehiclesResponse(b)
	if err != nil {
		errlogger.Println(err)
		return
	}
	if vehicles == nil {
		sleepHistory[route] += 1
		log.Println("No vehicles in response for route:", route)
		return
	}

	updated := FilterUpdatedVehicles(vehicles)

	if len(updated) > 0 {
		_, err = r.Table("vehicle_position").Insert(r.Expr(updated)).Run(session)
		if err != nil {
			errlogger.Println(err)
			return
		}
		log.Printf("Log %d vehicles, route %s.\n", len(updated), route)
	} else {
		log.Printf("No new vehicle positions to record for route %s.\n", route)
	}
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

func main() {
	for _, route := range routes {
		sleepHistory[route] = 0
	}

	session, err := r.Connect(connOpts)
	if err != nil {
		errlogger.Fatal(err)
	}
	log.Printf("Established connection to RethinkDB server at %s.\n", connOpts.Address)

	var wg sync.WaitGroup

	for {
		for _, route := range routes {
			wg.Add(1)
			go func(session *r.Session, route string) {
				LogVehiclePositions(session, route)
				wg.Done()
			}(session, route)
		}
		wg.Wait()

		var duration time.Duration
		if routesAreSleeping() {
			for k, _ := range sleepHistory {
				sleepHistory[k] = 0
			}
			log.Println("Sleeping for extended duration!")
			duration = extendedDuration
		} else {
			log.Println("Sleeping for normal duration!")
			duration = normalDuration
		}

		time.Sleep(duration)
		log.Println("Wake up!")
	}
}
