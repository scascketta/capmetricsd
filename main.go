package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/scascketta/capmetricsd/agency/capmetro"
	"github.com/scascketta/capmetricsd/task"

	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/kelseyhightower/envconfig"
)

var (
	dlog           = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog           = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cfg            = config{}
	cronitorClient = http.Client{Timeout: 10 * time.Second}
)

type config struct {
	CronitorURL string
	MaxRetries  int
}

func setupConn() *bolt.DB {
	db, err := bolt.Open("./test.db", 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		log.Fatal("Fatal error while opening bolt db: ", err)
	}
	return db
}

func LogVehiclesNotifyCronitor(setupConn func() *bolt.DB, fh *capmetro.FetchHistory) func() error {
	return func() error {
		res, err := cronitorClient.Get(cfg.CronitorURL)
		if err == nil {
			res.Body.Close()
		} else {
			return err
		}
		return capmetro.LogVehicleLocations(setupConn, fh)()
	}
}

func main() {
	err := envconfig.Process("cmdata", &cfg)
	if err != nil {
		elog.Fatal(err)
	}

	dlog.Println("config:", cfg)

	fh := capmetro.NewFetchHistory()

	locationTask := task.NewDynamicRepeatTask(
		LogVehiclesNotifyCronitor(setupConn, fh),
		30*time.Second,
		"LogVehiclesNotifyCronitor",
		capmetro.UpdateInterval(cfg.MaxRetries, fh),
	)

	task.StartTasks(locationTask)
}
