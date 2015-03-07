package task

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Objects implementing RepeatTasker run a task at a set interval
type RepeatTasker interface {
	RunTask()
	Interval() time.Duration
}

// RepeatTask calls Func at every Interval
type RepeatTask struct {
	Func     func() error
	interval time.Duration
	Name     string
	dlog     *log.Logger
	elog     *log.Logger
}

// RunTask() calls the RepeatTask's Func and writes any error to STDERR
func (rt *RepeatTask) RunTask() {
	rt.dlog.Println("Running task:", rt.Name)
	err := rt.Func()
	if err != nil {
		rt.elog.Println(err)
	}
	rt.dlog.Println("Next run in:", rt.Interval())
}

func (rt *RepeatTask) Interval() time.Duration {
	return rt.interval
}

func NewRepeatTask(fn func() error, interval time.Duration, name string) *RepeatTask {
	return &RepeatTask{
		Func:     fn,
		interval: interval,
		Name:     name,
		dlog:     log.New(os.Stdout, fmt.Sprintf("[DBG][TASK: %s] ", name), log.LstdFlags|log.Lshortfile),
		elog:     log.New(os.Stderr, fmt.Sprintf("[ERR][TASK: %s] ", name), log.LstdFlags|log.Lshortfile),
	}
}

// DynamicRepeatTask is like RepeatTask, but can change the interval by calling UpdateInterval
type DynamicRepeatTask struct {
	*RepeatTask
	UpdateInterval func() (bool, time.Duration)
}

// RunTask() calls the DynamicRepeatTask's Func and writes any error to STDERR
func (drt *DynamicRepeatTask) RunTask() {
	drt.dlog.Println("Running task: ", drt.Name)
	err := drt.Func()
	if err != nil {
		drt.elog.Println(err)
	}

	updated, d := drt.UpdateInterval()
	if updated {
		drt.dlog.Println("Updating interval")
		drt.interval = d
	} else {
		drt.dlog.Println("Not updating interval")
	}
	drt.dlog.Println("Next run in:", drt.Interval())
}

func (drt *DynamicRepeatTask) Interval() time.Duration {
	return drt.interval
}

func NewDynamicRepeatTask(fn func() error, interval time.Duration, name string, updateFn func() (bool, time.Duration)) *DynamicRepeatTask {
	return &DynamicRepeatTask{NewRepeatTask(fn, interval, name), updateFn}
}

// StartTasks starts calling RepeatTaskers in their own separate goroutine between their specified intervals. This runs forever.
func StartTasks(repeatTasks []RepeatTasker) {
	var wg sync.WaitGroup
	for _, rt := range repeatTasks {
		wg.Add(1)
		go func(rt RepeatTasker) {
			for {
				rt.RunTask()
				time.Sleep(rt.Interval())
			}
			wg.Done()
		}(rt)
	}
	wg.Wait()
}
