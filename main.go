package main

import (
	r "github.com/dancannon/gorethink"
	"log"
	"os"
	"time"
)

var (
	errlogger *log.Logger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	session   *r.Session
	connOpts  r.ConnectOpts
)

func init() {
	connOpts = r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "capmuerto",
	}
}

func LogVehiclePositions(route string, session *r.Session) {
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
}

func main() {
	session, err := r.Connect(connOpts)
	if err != nil {
		errlogger.Fatal(err)
	}

	for {
		LogVehiclePositions("803", session)
		go LogVehiclePositions("801", session)

		time.Sleep(90 * (1000 * time.Millisecond))
	}
}
