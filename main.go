package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/scascketta/capmetro-data/task"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/kelseyhightower/envconfig"
)

var (
	s        *r.Session
	dlog     = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog     = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cfg      = config{}
	extended = true
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

func cronitor() error {
	res, err := http.Get(cfg.CronitorURL)
	if err == nil {
		res.Body.Close()
	}
	return err
}

func UpdateInterval() (bool, time.Duration) {
	if routesAreSleeping() {
		for k := range staleResponses {
			staleResponses[k] = 0
		}
		log.Println("Sleeping for extended duration!")
		return true, extendedDuration
	} else {
		log.Println("Sleeping for normal duration!")
		return false, normalDuration
	}
}

func main() {
	err := envconfig.Process("cmdata", &cfg)
	if err != nil {
		elog.Fatal(err)
	}
	dlog.Println("config:", cfg)
	connOpts := r.ConnectOpts{Address: fmt.Sprintf("%s:%s", cfg.DbAddr, cfg.DbPort), Database: cfg.DbName}
	_, err = r.Connect(connOpts)
	if err != nil {
		elog.Fatal("Connection to RethinkDB failed: ", err)
	} else {
		dlog.Printf("Connection to RethinkDB at %s succeeded.\n", connOpts.Address)
	}

	cronitorTask := task.NewFixedRepeatTask(cronitor, 10*time.Minute, "NotifyCronitor")
	locationTask := task.NewDynamicRepeatTask(LogVehicleLocations, 30*time.Second, "LogVehicleLocations", UpdateInterval)
	repeatTasks := []task.RepeatTasker{locationTask, cronitorTask}

	var wg sync.WaitGroup
	for _, rt := range repeatTasks {
		wg.Add(1)
		go func(rt task.RepeatTasker) {
			for {
				rt.RunTask()
				time.Sleep(rt.Interval())
			}
			wg.Done()
		}(rt)
	}
	wg.Wait()
}
