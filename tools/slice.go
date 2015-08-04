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

func GetData(dbPath, dest string, min int64, max int64) error {
	minStr, maxStr := strconv.FormatInt(min, 10), strconv.FormatInt(max, 10)
	minIso, maxIso := time.Unix(min, 0).Format(Iso8601Format), time.Unix(max, 0).Format(Iso8601Format)
	log.Printf("Get data between %s and %s\n", minIso, maxIso)

	log.Println("dbPath: ", dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return err
	}

	locations := []agency.VehicleLocation{}

	err = db.View(func(tx *bolt.Tx) error {
		topBucket := tx.Bucket([]byte(capmetro.BucketName))

		err = topBucket.ForEach(func(tripID, _ []byte) error {
			tripBucket := topBucket.Bucket(tripID)
			c := tripBucket.Cursor()

			for k, v := c.Seek([]byte(minStr)); k != nil && bytes.Compare(k, []byte(maxStr)) <= 0; k, v = c.Next() {
				pbData := v

				var loc agency.VehicleLocation
				if err = proto.Unmarshal(pbData, &loc); err != nil {
					return err
				}

				locations = append(locations, loc)
			}

			return nil
		})

		return err
	})

	if err != nil {
		return err
	}

	log.Printf("Writing %d vehicle locations to %s.\n", len(locations), dest)

	coll := VehicleLocationCollection{
		Count:     len(locations),
		StartDate: minIso,
		EndDate:   maxIso,
		Data:      locations,
	}

	f, err := os.Create(dest)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if err = enc.Encode(coll); err != nil {
		dlog.Println(err)
		return err
	}

	return nil
}
