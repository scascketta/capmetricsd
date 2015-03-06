package task

import (
	"fmt"
	"log"
	"os"
	"time"
)

var (
	dlog = log.New(os.Stdout, "[DBG] ", log.LstdFlags|log.Lshortfile)
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

// Objects implementing RepeatTasker run a task at a set interval
type RepeatTasker interface {
	RunTask()
	Interval() time.Duration
}

// FixedRepeatTask calls Func at every Interval
type FixedRepeatTask struct {
	Func     func() error
	interval time.Duration
	Name     string
	dlog     *log.Logger
	elog     *log.Logger
}

func (frt *FixedRepeatTask) RunTask() {
	frt.dlog.Println("Running task:", frt.Name)
	err := frt.Func()
	if err != nil {
		frt.elog.Printf("[ERROR:%s]: %s\n", frt.Name, err.Error())
	}
	frt.dlog.Println("Next run in:", frt.Interval())
}

func (frt *FixedRepeatTask) Interval() time.Duration {
	return frt.interval
}

func NewFixedRepeatTask(fn func() error, interval time.Duration, name string) *FixedRepeatTask {
	frt := new(FixedRepeatTask)
	frt.Func = fn
	frt.interval = interval
	frt.Name = name
	frt.dlog = log.New(os.Stdout, fmt.Sprintf("[DBG][TASK: %s] ", name), log.LstdFlags|log.Lshortfile)
	frt.elog = log.New(os.Stderr, fmt.Sprintf("[ERR][TASK: %s] ", name), log.LstdFlags|log.Lshortfile)
	return frt
}

// DynamicRepeatTask calls a FixedRepeatTask.Func at every Interval which can be modified by UpdateInterval
type DynamicRepeatTask struct {
	Func           func() error
	interval       time.Duration
	Name           string
	dlog           *log.Logger
	elog           *log.Logger
	UpdateInterval func() (bool, time.Duration)
}

func (drt *DynamicRepeatTask) RunTask() {
	drt.dlog.Println("Running task: ", drt.Name)
	err := drt.Func()
	if err != nil {
		drt.elog.Printf("[ERROR:%s] %s\n", drt.Name, err.Error())
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
	drt := new(DynamicRepeatTask)
	drt.Func = fn
	drt.interval = interval
	drt.Name = name
	drt.UpdateInterval = updateFn
	drt.dlog = log.New(os.Stdout, fmt.Sprintf("[DBG][TASK: %s] ", name), log.LstdFlags|log.Lshortfile)
	drt.elog = log.New(os.Stderr, fmt.Sprintf("[ERR][TASK: %s] ", name), log.LstdFlags|log.Lshortfile)
	return drt
}
