package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"sync"
	"time"

	r "github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/dancannon/gorethink"
	"github.com/scascketta/capmetro-data/Godeps/_workspace/src/github.com/kelseyhightower/envconfig"
)

var (
	s    *r.Session
	dlog = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
	cfg  = config{}
)

type config struct {
	DbName, DbAddr, DbPort string
	CronitorURL            string
	MaxRetries             int
}

// RepeatTask calls Func at every Interval
type RepeatTask struct {
	Func     func() error
	Interval time.Duration
	Name     string
}

func (rt *RepeatTask) Run() {
	dlog.Println("Running task:", rt.Name)
	err := rt.Func()
	if err != nil {
		elog.Printf("[ERROR:%s]: %s\n", rt.Name, err.Error())
	}
}

func runTasksOnce(tasks []RepeatTask) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t RepeatTask) {
			t.Run()
			wg.Done()
		}(t)
	}
	wg.Wait()
}

func setupConn() *r.Session {
	s, err := r.Connect(r.ConnectOpts{Address: fmt.Sprintf("%s:%s", cfg.DbAddr, cfg.DbPort), Database: cfg.DbName})
	if err != nil {
		elog.Fatal(err)
	}
	return s
}

// DynamicRepeatTask calls a RepeatTask.Func at every Interval which can be modified by UpdateInterval
type DynamicRepeatTask struct {
	Task           RepeatTask
	UpdateInterval func() (time.Duration, bool)
}

func cronitor() error {
	res, err := http.Get(cfg.CronitorURL)
	if err == nil {
		res.Body.Close()
	}
	return err
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

	cronitorTask := RepeatTask{
		Func:     cronitor,
		Interval: 10 * time.Minute,
		Name:     "NotifyCronitor",
	}
	locationTask := RepeatTask{
		Func:     LogVehicleLocations,
		Interval: 30 * time.Second,
		Name:     "LogVehicleLocations",
	}

	tasks := []RepeatTask{locationTask, cronitorTask}
	runTasksOnce(tasks)

	cases := make([]reflect.SelectCase, len(tasks))
	for i, t := range tasks {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.NewTicker(t.Interval).C),
		}
	}

	for {
		chosen, _, _ := reflect.Select(cases)
		go tasks[chosen].Run()
	}
}
