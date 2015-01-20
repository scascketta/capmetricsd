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
	connOpts  r.ConnectOpts
	routes    []string = []string{"803", "801", "550"}
)

func init() {
	connOpts = r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "capmuerto",
	}
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

	for _, v := range vehicles {
		_, err = r.Table("vehicle_position").Insert(r.Expr(v)).Run(session)
		if err != nil {
			errlogger.Println(err)
		}
	}
	log.Printf("Log %d vehicles on route %s.\n", len(vehicles), route)
}

func main() {
	session, err := r.Connect(connOpts)
	if err != nil {
		errlogger.Fatal(err)
	}

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
		time.Sleep(90 * (1000 * time.Millisecond))
		log.Println("Wake up!")
	}
}
