package daemon

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/scascketta/capmetricsd/daemon/agency/capmetro"
	"github.com/scascketta/capmetricsd/daemon/task"

	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/scascketta/envconfig"
)

var (
	dlog           = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog           = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cfg            = Config{}
	cronitorClient = http.Client{Timeout: 10 * time.Second}
)

type Config struct {
	CronitorURL string `required:"true"`
	Retries     int    `required:"true"`
	DbPath      string `required:"true"`
}

func setupConn() *bolt.DB {
	db, err := bolt.Open(cfg.DbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
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

func Start() {
	if err := envconfig.Process("cm", &cfg); err != nil {
		elog.Fatal(err)
	}

	dlog.Println("config:", cfg)

	fh := capmetro.NewFetchHistory()

	locationTask := task.NewDynamicRepeatTask(
		LogVehiclesNotifyCronitor(setupConn, fh),
		30*time.Second,
		"LogVehiclesNotifyCronitor",
		capmetro.UpdateInterval(cfg.Retries, fh),
	)

	task.StartTasks(locationTask)
}
