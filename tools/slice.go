package tools

import (
	"bytes"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/daemon/agency"
	"github.com/scascketta/capmetricsd/daemon/agency/capmetro"
	"log"
	"os"
	"strconv"
	"time"
)

type VehicleLocationCollection struct {
	Count     int                      `json:"count"`
	StartDate string                   `json:"start_date"`
	EndDate   string                   `json:"end_date"`
	Data      []agency.VehicleLocation `json:"data"`
}

func readBoltData(db *bolt.DB, min, max string) (*[]agency.VehicleLocation, error) {
	locations := []agency.VehicleLocation{}

	err := db.View(func(tx *bolt.Tx) error {
		topBucket := tx.Bucket([]byte(capmetro.BucketName))

		err := topBucket.ForEach(func(tripID, _ []byte) error {
			tripBucket := topBucket.Bucket(tripID)
			c := tripBucket.Cursor()

			for k, v := c.Seek([]byte(min)); k != nil && bytes.Compare(k, []byte(max)) <= 0; k, v = c.Next() {
				var loc agency.VehicleLocation
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

func writeData(dest, min, max string, locations *[]agency.VehicleLocation) error {
	log.Printf("Writing %d vehicle locations to %s.\n", len(*locations), dest)

	coll := VehicleLocationCollection{
		Count:     len(*locations),
		StartDate: min,
		EndDate:   max,
		Data:      *locations,
	}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if err = enc.Encode(coll); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func GetData(dbPath, dest string, min int64, max int64) error {
	minStr, maxStr := strconv.FormatInt(min, 10), strconv.FormatInt(max, 10)
	minIso, maxIso := time.Unix(min, 0).Format(Iso8601Format), time.Unix(max, 0).Format(Iso8601Format)
	log.Printf("Get data between %s and %s\n", minIso, maxIso)

	log.Println("dbPath: ", dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return err
	}

	locations, err := readBoltData(db, minStr, maxStr)
	if err != nil {
		return err
	}

	err = writeData(dest, minIso, maxIso, locations)
	return err
}
