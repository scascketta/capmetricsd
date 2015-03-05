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
	dbglogger.Printf("Established connection to RethinkDB server at %s.\n", connOpts.Address)

	var wg sync.WaitGroup
	for {
		// Notify Cronitor
		go func() {
			res, err := http.Get(config.CronitorURL)
			if err != nil {
				errlogger.Println(err)
			} else {
				res.Body.Close()
			}
		}()

		session, err := r.Connect(connOpts)
		if err != nil {
			errlogger.Fatal(err)
		}

		// log new vehicle positions
		wg.Add(1)
		go func(session *r.Session) {
			err = LogVehicleLocations(session)
			if err != nil {
				errlogger.Println(err)
			}
			wg.Done()
		}(session)

		wg.Wait()

		// determine how long to sleep
		// if no vehicles were received from capmetro MAX_RETRIES in a row, sleep longer
		var duration time.Duration
		if routesAreSleeping() {
			for k := range staleResponses {
				staleResponses[k] = 0
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
