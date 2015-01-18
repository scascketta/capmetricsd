package main

import (
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func FetchVehicles(routeID string) ([]byte, error) {
	res, err := http.Get("http://www.capmetro.org/planner/s_buslocation.asp?route=" + routeID)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(res.Body)
}

type VehiclePosition struct {
	VehicleID string
	Direction string    `datastore:",noindex"` // N/S
	Time      time.Time // should be time position data was logged
	Speed     float64   `datastore:",noindex"` // instantaneous speed
	Heading   int64     `datastore:",noindex"` // heading in degrees
	Route     string    // 80X
	RouteID   string    // route id for machines
	TripID    string    // trip id for machines
	Latitude  float64
	Longitude float64
}

type xmlVehicle struct {
	VehicleID string   `xml:"Vehicleid"`
	Direction string   `xml:"Direction"`
	Time      string   `xml:"Updatetime"`
	Speed     float64  `xml:"Speed"`
	Heading   string   `xml:"Heading"`
	Route     string   `xml:"Route"`
	RouteID   string   `xml:"Routeid"`
	TripID    string   `xml:"Tripid"`
	Positions []string `xml:"Positions>Position"`
}

type VehicleData struct {
	Route    string       `xml:"Body>BuslocationResponse>Input>Route"`
	Vehicles []xmlVehicle `xml:"Body>BuslocationResponse>Vehicles>Vehicle"`
}

func ParseVehiclesResponse(b []byte) ([]VehiclePosition, error) {
	var data VehicleData
	err := xml.Unmarshal(b, &data)
	if err != nil {
		errlogger.Println(err)
	}

	var vehicles []VehiclePosition
	for _, v := range data.Vehicles {
		updateTime, err := parseUpdateTime(v.Time)
		if err != nil {
			return nil, err
		}
		heading, _ := strconv.ParseInt(v.Heading, 10, 64)
		heading *= 10
		latLon := strings.Split(v.Positions[0], ",")
		lat, _ := strconv.ParseFloat(latLon[0], 64)
		long, _ := strconv.ParseFloat(latLon[1], 64)
		vp := VehiclePosition{
			VehicleID: v.VehicleID,
			Direction: v.Direction,
			Route:     v.Route,
			RouteID:   v.RouteID,
			TripID:    v.TripID,
			Time:      updateTime,
			Speed:     v.Speed,
			Heading:   heading,
			Latitude:  lat,
			Longitude: long,
		}
		vehicles = append(vehicles, vp)
	}
	return vehicles, err
}

func parseUpdateTime(updatetime string) (time.Time, error) {
	now := time.Now()
	loc, _ := time.LoadLocation("America/Chicago")
	parsed, err := time.Parse("03:04 PM", updatetime)
	date := time.Date(now.Year(), now.Month(), now.Day(), parsed.Hour(), parsed.Minute(), 0, 0, loc)
	return date, err
}
