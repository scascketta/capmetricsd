package main

import (
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"
	"io/ioutil"
	"log"
)

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

	v, err := ParseVehiclesResponse(b)
	if err != nil {
		log.Fatal("Error parsing vehicles:", err)
	}
	for _, v := range v.Vehicles {
		log.Printf("Vehicle %s updated at %s, moving %s on route %s.\n", v.VehicleID, v.Time, v.Direction, v.Route)
		log.Printf("\tPositions:\n")
		for _, p := range v.Positions {
			fmt.Printf("%s\n", p)
		}
	}
}
