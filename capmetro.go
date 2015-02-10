package main

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
)

type Vehicle struct {
	ID           string    `gorethink:"id,omitempty"`
	VehicleID    string    `gorethink:"vehicle_id"`
	Route        string    `gorethink:"route"`         // 80X
	RouteID      string    `gorethink:"route_id"`      // route id for machines
	TripID       string    `gorethink:"trip_id"`       // trip id for machines
	LastAnalyzed time.Time `gorethink:"last_analyzed"` // last time vehicle was analyzed for stop times
}

type VehicleLocation struct {
	VehicleID string    `gorethink:"vehicle_id"`
	Time      time.Time `gorethink:"timestamp"` // should be time position data was logged
	Speed     float32   `gorethink:"speed"`     // instantaneous speed
	RouteID   string    `gorethink:"route_id"`  // 80X
	TripID    string    `gorethink:"trip_id"`   // trip id for machines
	Location  r.Term    `gorethink:"location"`
}

func FetchVehicles(route string) ([]VehicleLocation, error) {
	res, err := http.Get("https://data.texas.gov/download/i5qp-g5fd/application/octet-stream")
	if err != nil {
		return nil, err
	}
	b, _ := ioutil.ReadAll(res.Body)
	fm := new(FeedMessage)
	err = proto.Unmarshal(b, fm)

	var vehicles []VehicleLocation
	for _, entity := range fm.GetEntity() {
		vehicle := entity.GetVehicle()
		tripDescriptor := vehicle.GetTrip()
		position := vehicle.GetPosition()
		dbglogger.Println(vehicle.GetTimestamp())
		vl := VehicleLocation{
			VehicleID: vehicle.GetVehicle().GetId(),
			Time:      time.Unix(int64(vehicle.GetTimestamp()), 0),
			Speed:     position.GetSpeed(),
			RouteID:   tripDescriptor.GetRouteId(),
			TripID:    tripDescriptor.GetTripId(),
			Location:  r.Point(position.GetLongitude(), position.GetLatitude()),
		}
		vehicles = append(vehicles, vl)
	}
	return vehicles, err
}
