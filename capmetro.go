package main

import (
	"encoding/xml"
	r "github.com/scascketta/capmetro-log/Godeps/_workspace/src/github.com/dancannon/gorethink"
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

type Vehicle struct {
	VehicleID    string    `gorethink:"vehicle_id"`
	Route        string    `gorethink:"route"`         // 80X
	RouteID      string    `gorethink:"route_id"`      // route id for machines
	TripID       string    `gorethink:"trip_id"`       // trip id for machines
	LastAnalyzed time.Time `gorethink:"last_analyzed"` // last time vehicle was analyzed for stop times
}

type VehiclePosition struct {
	VehicleID string    `gorethink:"vehicle_id"`
	Direction string    `gorethink:"direction"` // N/S
	Time      time.Time `gorethink:"timestamp"` // should be time position data was logged
	Speed     float64   `gorethink:"speed"`     // instantaneous speed
	Heading   int64     `gorethink:"heading"`   // heading in degrees
	Route     string    `gorethink:"route"`     // 80X
	RouteID   string    `gorethink:"route_id"`  // route id for machines
	TripID    string    `gorethink:"trip_id"`   // trip id for machines
	Location  r.Term    `gorethink:"location"`
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
			Location:  r.Point(long, lat),
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
