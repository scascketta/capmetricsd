package daemon

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
)

const (
	LOG_INTERVAL   = 30 * time.Second
	BUCKET_NAME    = "vehicle_locations"
	ISO8601_FORMAT = "2006-01-02T15:04:05-07:00"
)

var (
	dlog           = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog           = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cronitorClient = http.Client{Timeout: 10 * time.Second}
)

func captureLocations(target, dbPath, cronitorURL string) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: LOG_INTERVAL})
	if err != nil {
		elog.Println("Error opening BoltDB: ", err.Error())
		return
	}
	defer db.Close()

	if err = CaptureLocations(target, db); err != nil {
		elog.Println(err)
		// if error is returned while recording location, don't notify cronitor
		return
	}

	notifyCronitor(cronitorURL)
}

func notifyCronitor(cronitorURL string) {
	if cronitorURL != "" {
		res, err := cronitorClient.Get(cronitorURL)
		if err != nil {
			elog.Printf("Error notifying Cronitor: %s\n", err.Error())
			return
		}
		res.Body.Close()
	}
}

func Start(target, cronitorURL, dbPath string) {
	captureLocations(target, dbPath, cronitorURL)

	ticker := time.Tick(LOG_INTERVAL)

	for {
		select {
		case <-ticker:
			captureLocations(target, dbPath, cronitorURL)
		}
	}
}
