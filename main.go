package main

import (
	r "github.com/scascketta/capmetro-log/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"log"
	"os"
	"sync"
	"time"
)

var (
	errlogger *log.Logger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	session   *r.Session
	DB_NAME   string        = os.Getenv("DB_NAME")
	connOpts  r.ConnectOpts = r.ConnectOpts{Address: "localhost:28015", Database: DB_NAME}
	routes    []string      = []string{"803", "801", "550"}

	sleepDuration time.Duration        = 30 * (1000 * time.Millisecond)
	lastUpdated   map[string]time.Time = map[string]time.Time{}
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
	}
	vehicles, err := ParseVehiclesResponse(b)
	if err != nil {
		errlogger.Println(err)
	}

	updated := FilterUpdatedVehicles(vehicles)

	if len(updated) > 0 {
		_, err = r.Table("vehicle_position").Insert(r.Expr(updated)).Run(session)
		if err != nil {
			errlogger.Println(err)
		}
		log.Printf("Log %d vehicles, route %s.\n", len(updated), route)
	} else {
		log.Printf("No new vehicle positions to record for route %s.\n", route)
	}
}

func main() {
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

		log.Println("Sleeping...")
		time.Sleep(sleepDuration)
		log.Println("Wake up!")
	}
}
