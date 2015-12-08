package tools

import (
	"bytes"
	"encoding/csv"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/daemon"
	"github.com/scascketta/capmetricsd/daemon/gtfsrt"

	"log"
	"os"
	"strconv"
	"time"
)

func readBoltData(db *bolt.DB, min, max string) (*[]gtfsrt.VehicleLocation, error) {
	locations := []gtfsrt.VehicleLocation{}

	err := db.View(func(tx *bolt.Tx) error {
		topBucket := tx.Bucket([]byte(daemon.BUCKET_NAME))

		err := topBucket.ForEach(func(tripID, _ []byte) error {
			tripBucket := topBucket.Bucket(tripID)
			c := tripBucket.Cursor()

			for k, v := c.Seek([]byte(min)); k != nil && bytes.Compare(k, []byte(max)) <= 0; k, v = c.Next() {
				var loc gtfsrt.VehicleLocation
				if err := proto.Unmarshal(v, &loc); err != nil {
					return err
				}

				locations = append(locations, loc)
			}

			return nil
		})

		return err
	})

	return &locations, err
}

func writeData(dest string, locations *[]gtfsrt.VehicleLocation) error {
	log.Printf("Writing %d vehicle locations to %s.\n", len(*locations), dest)

	headers := []string{"vehicle_id", "timestamp", "speed", "route_id", "trip_id", "latitude", "longitude"}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err = w.Write(headers); err != nil {
		log.Println("Error writing CSV header record")
		return err
	}

	for _, loc := range *locations {
		t := time.Unix(loc.GetTimestamp(), 0).UTC()
		record := []string{
			loc.GetVehicleId(),
			t.Local().Format(Iso8601Format),
			strconv.FormatFloat(float64(loc.GetSpeed()), 'f', -1, 32),
			loc.GetRouteId(),
			loc.GetTripId(),
			strconv.FormatFloat(float64(loc.GetLatitude()), 'f', -1, 32),
			strconv.FormatFloat(float64(loc.GetLongitude()), 'f', -1, 32),
		}
		if err = w.Write(record); err != nil {
			log.Println("Error writing CSV records")
			return err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

func GetData(dbPath, dest string, min string, max string) error {
	log.Printf("Get data between %s and %s\n", min, max)

	log.Println("dbPath: ", dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return err
	}

	locations, err := readBoltData(db, min, max)
	if err != nil {
		return err
	}

	err = writeData(dest, locations)
	return err
}
