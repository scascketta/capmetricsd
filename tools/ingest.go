package tools

import (
	"bytes"
	"encoding/csv"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/cheggaaa/pb"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/golang/protobuf/proto"
	"github.com/scascketta/capmetricsd/daemon/agency"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	Iso8601Format = "2006-01-02T15:04:05-07:00"
	BucketName    = "vehicle_locations"
)

func countLines(fname string) (int, error) {
	r, err := os.Open(fname)
	if err != nil {
		log.Println(err)
	}
	defer r.Close()

	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}

func countBucketKeys(db *bolt.DB) int {
	count := 0
	db.Update(func(tx *bolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte(BucketName))

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count += 1
		}
		return nil
	})
	return count
}

// assumes CSV field names in order
// vehicle_id,dist_traveled,speed,lon,route_id,trip_headsign,timestamp,lat,trip_id
func unmarshalCSV(record []string) *agency.VehicleLocation {
	vehicleID, speedStr, lonStr, routeID, timeStr, latStr, tripID := record[0], record[2], record[3], record[4], record[6], record[7], record[8]

	speed, _ := strconv.ParseFloat(speedStr, 32)
	lon, _ := strconv.ParseFloat(lonStr, 32)
	lat, _ := strconv.ParseFloat(latStr, 32)
	timestamp, _ := time.Parse(Iso8601Format, timeStr)

	loc := &agency.VehicleLocation{
		VehicleId: proto.String(vehicleID),
		Timestamp: proto.Int64(timestamp.Unix()),
		Speed:     proto.Float32(float32(speed)),
		RouteId:   proto.String(routeID),
		TripId:    proto.String(tripID),
		Latitude:  proto.Float32(float32(lat)),
		Longitude: proto.Float32(float32(lon)),
	}
	return loc
}

func storeBolt(record []string) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		topBucket, err := tx.CreateBucketIfNotExists([]byte(BucketName))
		if err != nil {
			return err
		}

		loc := unmarshalCSV(record)

		tripID := loc.GetTripId()

		tripBucket, err := topBucket.CreateBucketIfNotExists([]byte(tripID))
		if err != nil {
			return err
		}

		data, err := proto.Marshal(loc)
		if err != nil {
			return err
		}

		ts := loc.GetTimestamp()
		key := strconv.Itoa(int(ts))

		err = tripBucket.Put([]byte(key), data)
		if err != nil {
			return err
		}
		return nil
	}
}

func ProcessFile(fname string, db *bolt.DB) (error, int) {
	log.Printf("Reading data from %s\n", fname)

	file, err := os.Open(fname)
	if err != nil {
		return err, 0
	}
	defer file.Close()

	rdr := csv.NewReader(file)
	start := time.Now()

	records, err := rdr.ReadAll()
	if err != nil {
		return err, 0
	}

	log.Printf("Records to ingest: %d\n", len(records))

	pbar := pb.StartNew(len(records) - 1)

	var wg sync.WaitGroup
	// FIXME: Buffer N records at a time
	for _, record := range records {
		wg.Add(1)

		go func(record []string) {
			err = db.Batch(storeBolt(record))
			if err != nil {
				log.Fatal(err)
			}
			pbar.Increment()
			wg.Done()
		}(record)
	}

	wg.Wait()

	elapsed := time.Now().Sub(start).Seconds()
	log.Printf("Rows ingested per second: %f\n", float64(len(records))/elapsed)

	return nil, len(records)
}

func Ingest(pattern string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	db, err := bolt.Open(os.Getenv("CAPMETRICSDB"), 0600, nil)
	if err != nil {
		elog.Fatal(err)
	}

	defer db.Close()
	files, _ := filepath.Glob(pattern)
	log.Printf("# of Files found with pattern %s: %v\n", pattern, len(files))
	total := 0
	start := time.Now()

	for _, fname := range files {
		err, count := ProcessFile(fname, db)
		if err != nil {
			log.Fatal(err)
		}
		total += count
	}

	log.Printf("Total records (apparently) written: %d\n", total)

	checkCount := 0
	db.View(func(tx *bolt.Tx) error {
		topBucket := tx.Bucket([]byte(BucketName))
		topBucket.ForEach(func(trip, _ []byte) error {
			tripBucket := topBucket.Bucket(trip)
			tripBucket.ForEach(func(timestamp, val []byte) error {
				checkCount++
				return nil
			})
			return nil
		})
		return nil
	})
	log.Printf("Total records read: %d\n", checkCount)

	elapsed := time.Now().Sub(start).Seconds()
	log.Printf("Total time elapsed: %fs, average # of rows ingested per second: %f\n", elapsed, float64(checkCount)/elapsed)
}
