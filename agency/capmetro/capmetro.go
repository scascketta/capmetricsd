package capmetro

import (
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/agency"
	"io/ioutil"
	"net/http"
)

// RouteLocations contains vehicle locations on a certain route.
type RouteLocations struct {
	Route     string
	Locations []agency.VehicleLocation
	Updated   bool
	Empty     bool
}

// NewRouteLocations returns a new RouteLocations struct with the given route.
func NewRouteLocations(route string) RouteLocations {
	var rl RouteLocations
	rl.Locations = make([]agency.VehicleLocation, 0)
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

	body, _ := ioutil.ReadAll(res.Body)

	fm := new(agency.FeedMessage)
	if err = proto.Unmarshal(body, fm); err != nil {
		return nil, err
	}

	var locations []agency.VehicleLocation

	for _, entity := range fm.GetEntity() {
		vehicle := entity.GetVehicle()
		trip := vehicle.GetTrip()
		position := vehicle.GetPosition()

		loc := agency.VehicleLocation{
			VehicleId: proto.String(vehicle.GetVehicle().GetId()),
			Timestamp: proto.Int64(int64(vehicle.GetTimestamp())),
			Speed:     proto.Float32(position.GetSpeed()),
			RouteId:   proto.String(trip.GetRouteId()),
			TripId:    proto.String(trip.GetTripId()),
			Latitude:  proto.Float32(position.GetLatitude()),
			Longitude: proto.Float32(position.GetLongitude()),
		}

		locations = append(locations, loc)
	}

	vehiclesByRoute := sortByRoute(locations)

	return vehiclesByRoute, nil
}

// sortByRoute returns the given vehicles sorted by route
func sortByRoute(locations []agency.VehicleLocation) map[string]RouteLocations {
	sorted := make(map[string]RouteLocations)

	for _, loc := range locations {
		route := loc.GetRouteId()
		rl, ok := sorted[route]
		if !ok {
			rl = NewRouteLocations(route)
		}
		rl.Locations = append(rl.Locations, loc)
		sorted[route] = rl
	}
	return sorted
}
