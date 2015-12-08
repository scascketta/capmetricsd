package daemon

import (
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/montanaflynn/stats"
	"github.com/scascketta/capmetricsd/daemon/gtfsrt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

type locationBins map[string][]*gtfsrt.VehicleLocation

func CaptureLocations(url string, db *bolt.DB) (err error) {
	pb, err := getLocations(url)
	if err != nil {
		return
	}

	locations, err := decodeProtobuf(pb)
	if err != nil {
		return
	}

	filtered := filterLocations(locations)

	tripBins := binLocations(filtered)

	if err = storeLocations(db, tripBins); err != nil {
		return
	}

	describeLocations(filtered)
	printStats(len(locations), len(filtered), len(tripBins))

	return
}

func getLocations(url string) (pb []byte, err error) {
	start := time.Now()
	pb = []byte{}

	res, err := http.Get(url)
	if err != nil {
		return
	}

	pb, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	res.Body.Close()

	end := time.Now().Sub(start)
	dlog.Printf("Time elapsed downloading PB file: %.0fms\n", end.Seconds()*1000)
	return
}

func decodeProtobuf(pb []byte) (locations []*gtfsrt.VehicleLocation, err error) {
	start := time.Now()
	fm := new(gtfsrt.FeedMessage)
	if err = proto.Unmarshal(pb, fm); err != nil {
		return nil, err
	}

	for _, entity := range fm.GetEntity() {
		vehicle := entity.GetVehicle()
		trip := vehicle.GetTrip()
		position := vehicle.GetPosition()

		loc := &gtfsrt.VehicleLocation{
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

	end := time.Now().Sub(start)
	dlog.Printf("Time elapsed decoding PB file: %.0fms\n", end.Seconds()*1000)

	return locations, nil
}

func filterLocations(locations []*gtfsrt.VehicleLocation) []*gtfsrt.VehicleLocation {
	var filtered []*gtfsrt.VehicleLocation

	for _, location := range locations {
		active := location.GetRouteId() != "" && location.GetVehicleId() != "" && location.GetTripId() != ""
		if active {
			filtered = append(filtered, location)
		}
	}

	return filtered
}

func binLocations(locations []*gtfsrt.VehicleLocation) locationBins {
	bins := locationBins{}

	for _, loc := range locations {
		trip := loc.GetTripId()
		if _, ok := bins[trip]; !ok {
			bins[trip] = append(bins[trip], loc)
		}
	}

	return bins
}

func describeLocations(locations []*gtfsrt.VehicleLocation) {
	if len(locations) == 0 {
		return
	}

	var timestamps []float64
	var speeds []float64
	for _, location := range locations {
		timestamps = append(timestamps, float64(location.GetTimestamp()))
		speeds = append(speeds, float64(location.GetSpeed()))
	}

	medianTime, _ := stats.Median(timestamps)
	meanSpeed, _ := stats.Median(speeds)

	dlog.Printf("Median timestamp: %s", time.Unix(int64(medianTime), 0).Format(ISO8601_FORMAT))
	dlog.Printf("Mean speed: %f", meanSpeed)
}

func storeLocations(db *bolt.DB, tripBins locationBins) error {
	start := time.Now()
	for trip, locations := range tripBins {
		if trip == "" {
			continue
		}
		tripBytes := []byte(trip)
		for _, location := range locations {
			err := db.Update(func(tx *bolt.Tx) error {
				return storeSingleLocation(tripBytes, location, tx)
			})

			if err != nil {
				return err
			}
		}

	}
	end := time.Now().Sub(start)
	dlog.Printf("Time elapsed saving locations to BoltDB:  %.0fms\n", end.Seconds()*1000)
	return nil
}

func storeSingleLocation(tripID []byte, location *gtfsrt.VehicleLocation, tx *bolt.Tx) (err error) {
	topBucket, err := tx.CreateBucketIfNotExists([]byte(BUCKET_NAME))
	if err != nil {
		return
	}
	tripBucket, err := topBucket.CreateBucketIfNotExists(tripID)
	if err != nil {
		return
	}

	data, err := proto.Marshal(location)
	if err != nil {
		return
	}

	// key is POSIX time
	ts := location.GetTimestamp()
	key := strconv.Itoa(int(ts))

	err = tripBucket.Put([]byte(key), data)
	if err != nil {
		elog.Fatal(err)
	}
	return
}

func printStats(numLocations, numFiltered, numTrips int) {
	dlog.Printf("Locations: %d\n", numLocations)
	dlog.Printf("Valid locations: %d\n", numFiltered)
	dlog.Printf("Valid trips: %d\n", numTrips)
}
