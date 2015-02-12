package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/kelseyhightower/envconfig"
)

var (
	session   *r.Session
	dbglogger = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	errlogger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	config    = Config{}
	routes    = []string{"803", "801", "550"}
)

// Config contains all configuration
type Config struct {
	DbName, DbAddr, DbPort  string
	CronitorURL             string
	MaxDistance, MaxRetries int
}

func main() {
	err := envconfig.Process("cmdata", &config)
	if err != nil {
		errlogger.Fatal(err)
	}
	dbglogger.Println("Config:", config)
	connOpts := r.ConnectOpts{Address: fmt.Sprintf("%s:%s", config.DbAddr, config.DbPort), Database: config.DbName}

	// initialize current fetch retries to 0
	for _, route := range routes {
		emptyResponses[route] = 1
		recentEmptyResponse[route] = false
	}

	dbglogger.Printf("Established connection to RethinkDB server at %s.\n", connOpts.Address)

	go func() {
		for {
			session, err := r.Connect(connOpts)
			if err != nil {
				errlogger.Println(err)
			}
			dbglogger.Println("Make vehicle stop times")
			if err := MakeVehicleStopTimes(session); err != nil {
				errlogger.Println(err)
			}
			session.Close()
			time.Sleep(2 * time.Minute)
		}
	}()

	var wg sync.WaitGroup

	for {
		// Notify Cronitor
		go func() {
			res, err := http.Get(config.CronitorURL)
			if err != nil {
				errlogger.Println(err)
			}
			res.Body.Close()
		}()

		session, err := r.Connect(connOpts)
		if err != nil {
			errlogger.Fatal(err)
		}

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
			for k := range emptyResponses {
				emptyResponses[k] = 0
			}
			dbglogger.Println("Sleeping for extended duration!")
			duration = extendedDuration
		} else {
			dbglogger.Println("Sleeping for normal duration!")
			duration = normalDuration
		}

		session.Close()
		time.Sleep(duration)
		dbglogger.Println("Wake up!")
	}
}
