package task

import (
	"log"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[TASK] ")
}

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
}

func (frt *FixedRepeatTask) RunTask() {
	log.Println("Running task:", frt.Name)
	err := frt.Func()
	if err != nil {
		log.Printf("[ERROR:%s]: %s\n", frt.Name, err.Error())
	}
	log.Println("Next run in:", frt.Interval())
}

func (frt *FixedRepeatTask) Interval() time.Duration {
	return frt.interval
}

func NewFixedRepeatTask(fn func() error, interval time.Duration, name string) *FixedRepeatTask {
	frt := new(FixedRepeatTask)
	frt.Func = fn
	frt.interval = interval
	frt.Name = name
	return frt
}

// DynamicRepeatTask calls a FixedRepeatTask.Func at every Interval which can be modified by UpdateInterval
type DynamicRepeatTask struct {
	Func           func() error
	interval       time.Duration
	Name           string
	UpdateInterval func() (bool, time.Duration)
}

func (drt *DynamicRepeatTask) RunTask() {
	log.Println("Running task: ", drt.Name)
	err := drt.Func()
	if err != nil {
		log.Printf("[ERROR:%s] %s\n", drt.Name, err.Error())
	}

	updated, d := drt.UpdateInterval()
	if updated {
		log.Println("Updating interval")
		drt.interval = d
	} else {
		log.Println("Not updating interval")
	}
	log.Println("Next run in:", drt.Interval())
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
	return drt
}
