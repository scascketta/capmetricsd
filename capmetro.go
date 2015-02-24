package main

import (
	"io/ioutil"
	"net/http"
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/golang/protobuf/proto"
)

// Vehicle describes a transit vehicle.
type Vehicle struct {
	ID           string    `gorethink:"id,omitempty"`
	VehicleID    string    `gorethink:"vehicle_id"`
	Route        string    `gorethink:"route"`         // human_readable route string
	RouteID      string    `gorethink:"route_id"`      // route id for machines
	TripID       string    `gorethink:"trip_id"`       // trip id for machines
	LastAnalyzed time.Time `gorethink:"last_analyzed"` // last time vehicle was analyzed for stop times
}

// VehicleLocation is the recorded location of a vehicle at an instance in time.
type VehicleLocation struct {
	VehicleID string    `gorethink:"vehicle_id"`
	Time      time.Time `gorethink:"timestamp"` // should be time position data was logged
	Speed     float32   `gorethink:"speed"`     // instantaneous speed
	RouteID   string    `gorethink:"route_id"`  // 80X
	TripID    string    `gorethink:"trip_id"`   // trip id for machines
	Bearing   float32   `gorethink:"bearing"`
	Location  r.Term    `gorethink:"location"`
}

// RouteLocations contains vehicle locations on a certain route.
type RouteLocations struct {
	Route     string
	Locations []VehicleLocation
	Updated   bool
	Empty     bool
}

// NewRouteLocations returns a new RouteLocations struct with the given route.
func NewRouteLocations(route string) RouteLocations {
	var rl RouteLocations
	rl.Locations = make([]VehicleLocation, 0)
	rl.Route = route
	rl.Updated = false
	return rl
}

// FetchVehicles fetches vehicle locations and returns RouteLocations sorted by route and an error, if any.
func FetchVehicles() (map[string]RouteLocations, error) {
	res, err := http.Get("https://data.texas.gov/download/i5qp-g5fd/application/octet-stream")
	if err != nil {
		return nil, err
	}
	b, _ := ioutil.ReadAll(res.Body)
	fm := new(FeedMessage)
	err = proto.Unmarshal(b, fm)
	var locations []VehicleLocation
	for _, entity := range fm.GetEntity() {
		vehicle := entity.GetVehicle()
		trip := vehicle.GetTrip()
		position := vehicle.GetPosition()
		vl := VehicleLocation{
			VehicleID: vehicle.GetVehicle().GetId(),
			Time:      time.Unix(int64(vehicle.GetTimestamp()), 0),
			Speed:     position.GetSpeed(),
			Bearing:   position.GetBearing(),
			RouteID:   trip.GetRouteId(),
			TripID:    trip.GetTripId(),
			Location:  r.Point(position.GetLongitude(), position.GetLatitude()),
		}
		locations = append(locations, vl)
	}
	vehiclesByRoute := sortVehicles(locations)
	return vehiclesByRoute, err
}

// sortVehicles returns the given vehicles sorted by route
func sortVehicles(locations []VehicleLocation) map[string]RouteLocations {
	sorted := make(map[string]RouteLocations)
	for _, loc := range locations {
		route := loc.RouteID
		rl, ok := sorted[route]
		if !ok {
			rl = NewRouteLocations(route)
		}
		rl.Locations = append(rl.Locations, loc)
		sorted[route] = rl
	}
	return sorted
}
