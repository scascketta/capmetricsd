package capmetro

import (
	gtfsrt "github.com/scascketta/CapMetrics/Godeps/_workspace/src/gist.github.com/scascketta/fcced4a6518f68189666"
	"github.com/scascketta/CapMetrics/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"io/ioutil"
	"net/http"
	"time"
)

// VehicleLocation is the recorded location of a vehicle at an instance in time.
type VehicleLocation struct {
	VehicleID string    `json:"vehicle_id"`
	Time      time.Time `json:"time"`
	Speed     float32   `json:"speed"`
	RouteID   string    `json:"route_id"`
	TripID    string    `json:"trip_id"`
	Bearing   float32   `json:"bearing"`
	Latitude  float32   `json:"latitude"`
	Longitude float32   `json:"longitude"`
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
	fm := new(gtfsrt.FeedMessage)
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
			Longitude: position.GetLongitude(),
			Latitude:  position.GetLatitude(),
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
