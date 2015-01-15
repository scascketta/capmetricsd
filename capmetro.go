package main

import (
	"encoding/xml"
	"io/ioutil"
	"net/http"
)

func FetchVehicles(routeID string) ([]byte, error) {
	res, err := http.Get("http://www.capmetro.org/planner/s_buslocation.asp?route=" + routeID)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(res.Body)
}

type Vehicle struct {
	VehicleID string   `xml:"Vehicleid"`
	Direction string   `xml:"Direction"`
	Time      string   `xml:"Updatetime"`
	Speed     string   `xml:"Speed"`
	Heading   string   `xml:"Heading"`
	Route     string   `xml:"Route"`
	RouteID   string   `xml:"Routeid"`
	TripID    string   `xml:"Tripid"`
	Positions []string `xml:"Positions>Position"`
}

type VehicleData struct {
	Route    string    `xml:"Body>BuslocationResponse>Input>Route"`
	Vehicles []Vehicle `xml:"Body>BuslocationResponse>Vehicles>Vehicle"`
}

func ParseVehiclesResponse(b []byte) (VehicleData, error) {
	var data VehicleData
	err := xml.Unmarshal(b, &data)
	return data, err
}
