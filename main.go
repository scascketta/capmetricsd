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

func main() {
	_, err := AuthGCD()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("auth works")

	b, err := FetchVehicles("803")
	if err != nil {
		log.Fatal("Error fetching vehicles:", err)
	}

	vehicles, err := ParseVehiclesResponse(b)
	if err != nil {
		log.Fatal("Error parsing vehicles:", err)
	}

	for _, v := range vehicles {
		log.Printf("Vehicle %s was at %f, %f. at %s.", v.VehicleID, v.Latitude, v.Longitude, v.Time.String())
	}
}
