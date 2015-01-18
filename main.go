package main

import (
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var errlogger *log.Logger = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)

func AuthGCD() (context.Context, error) {
	jsonKey, err := ioutil.ReadFile("keyfile.json")
	if err != nil {
		return nil, err
	}

	conf, err := google.JWTConfigFromJSON(
		oauth2.NoContext,
		jsonKey,
		datastore.ScopeDatastore,
		datastore.ScopeUserEmail,
	)
	if err != nil {
		return nil, err
	}

	ctx := cloud.NewContext("concise-bloom-825", conf.Client(oauth2.NoContext))
	return ctx, nil
}

func LogVehiclePositions(ctx context.Context, route string) error {
	b, err := FetchVehicles(route)
	if err != nil {
		return err
	}

	vehicles, err := ParseVehiclesResponse(b)
	if err != nil {
		return err
	}

	for _, v := range vehicles {
		log.Printf("Vehicle %s was at %f, %f. at %s.", v.VehicleID, v.Latitude, v.Longitude, v.Time.String())
		k := datastore.NewIncompleteKey(ctx, "VehiclePosition", nil)
		_, err := datastore.Put(ctx, k, &v)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	ctx, err := AuthGCD()
	if err != nil {
		errlogger.Fatal(err)
	}
	for {
		err = LogVehiclePositions(ctx, "803")
		if err != nil {
			errlogger.Println(err)
		}
		err = LogVehiclePositions(ctx, "801")
		time.Sleep(90 * (time.Millisecond * 1000))
	}
}
