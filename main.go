package main

import (
	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
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
)

func init() {
	if len(os.Getenv("DB_NAME")) == 0 {
		errlogger.Fatal("Missing envvar DB_NAME")
	}
}

func main() {
	// initialize current fetch retries to 0
	for _, route := range routes {
		emptyResponses[route] = 1
		recentEmptyResponse[route] = false
	}

	session, err := r.Connect(connOpts)
	if err != nil {
		errlogger.Fatal(err)
	}
	dbglogger.Printf("Established connection to RethinkDB server at %s.\n", connOpts.Address)

	go func() {
		for {
			dbglogger.Println("Make vehicle stop times")
			// TODO: make this concurrent later
			if err := MakeVehicleStopTimes(session); err != nil {
				errlogger.Fatal(err)
			}
			time.Sleep(60 * 60 * (1000 * time.Millisecond))
		}
	}()

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
			for k, _ := range emptyResponses {
				emptyResponses[k] = 0
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
