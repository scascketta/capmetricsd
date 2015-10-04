package tools

import (
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	MinTime = time.Unix(21600, 0)
	MaxTime = time.Unix(1893477600, 0)
	elog    = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

func PrintBoltStats(path string) error {
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {
		log.Printf("Inspecting keys in bucket: %s\n", BucketName)
		b := tx.Bucket([]byte(BucketName))
		if b == nil {
			elog.Fatalf("Nonexistent bucket: %s\n", BucketName)
		}
		keys := 0
		minTime := MaxTime
		maxTime := MinTime
		err := b.ForEach(func(trip, _ []byte) error {
			tripBucket := b.Bucket(trip)
			return tripBucket.ForEach(func(timeBytes, _ []byte) error {
				keys++
				timeInt, err := strconv.ParseInt(string(timeBytes), 10, 64)
				if err != nil {
					return err
				}
				t := time.Unix(int64(timeInt), 0).UTC()

				if t.Before(minTime) {
					minTime = t
				}
				if t.After(maxTime) {
					maxTime = t
				}
				return nil
			})
		})
		if err != nil {
			return err
		}

		log.Printf("Number of keys: %d\n", keys)
		log.Printf("Smallest timestamp: %s", minTime.Local().Format(Iso8601Format))
		log.Printf("Largest timestamp: %s", maxTime.Local().Format(Iso8601Format))
		return nil
	})
}
