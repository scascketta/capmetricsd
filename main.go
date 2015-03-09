package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/scascketta/CapMetrics/agency/capmetro"
	"github.com/scascketta/CapMetrics/task"

	r "github.com/scascketta/CapMetrics/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"github.com/scascketta/CapMetrics/Godeps/_workspace/src/github.com/kelseyhightower/envconfig"
)

var (
	dlog           = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog           = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cfg            = config{}
	cronitorClient = http.Client{Timeout: 10 * time.Second}
)

type config struct {
	DbName, DbAddr, DbPort string
	CronitorURL            string
	MaxRetries             int
}

func setupConn() *r.Session {
	s, err := r.Connect(r.ConnectOpts{Address: fmt.Sprintf("%s:%s", cfg.DbAddr, cfg.DbPort), Database: cfg.DbName})
	if err != nil {
		elog.Fatal(err)
	}
	return s
}

func LogVehiclesNotifyCronitor(setupConn func() *r.Session, fh *capmetro.FetchHistory) func() error {
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
	s := setupConn()
	s.Close()

	fh := capmetro.NewFetchHistory()
	locationTask := task.NewDynamicRepeatTask(LogVehiclesNotifyCronitor(setupConn, fh), 30*time.Second, "LogVehiclesNotifyCronitor", capmetro.UpdateInterval(cfg.MaxRetries, fh))
	task.StartTasks(locationTask)
}
