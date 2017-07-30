package parallel

import (
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/rai-project/uuid"
)

type Parallel interface {
	Start() Parallel
	Wait()
	Stop()
	Add(Task)
	Run()
}

type Task interface {
	Cancelable() bool
	Run()
}

type ParallelImpl struct {
	uuid        string
	tasks       chan Task
	maxRoutines int
	looping     bool
	wg          sync.WaitGroup
}

type NonCancelableTask struct {
}

func (NonCancelableTask) Cancelable() bool {
	return false
}

type NonCancelableTaskFunc func()

func (NonCancelableTaskFunc) Cancelable() bool {
	return false
}
func (f NonCancelableTaskFunc) Run() {
	f()
}

func (parallel *ParallelImpl) loop() {
	defer func() {
		if r := recover(); r != nil {
			log.WithField("uuid", parallel.uuid).
				WithField("stack", string(debug.Stack())).
				WithField("recover", r).
				Error("panic")

			go parallel.loop()
		}
	}()

	defer parallel.wg.Done()

	for parallel.looping {
		select {
		case task := <-parallel.tasks:
			if len(parallel.tasks) == parallel.maxRoutines {
				if task.Cancelable() {
					continue
				}
			}

			task.Run()
		}
	}
}

func (p *ParallelImpl) Add(task Task) {
	p.wg.Add(1)
	p.tasks <- task
}

func (p *ParallelImpl) Start() Parallel {
	p.looping = true
	for index := 0; index < p.maxRoutines; index++ {
		go p.loop()
	}

	return p
}

func (p *ParallelImpl) Stop() {
	p.looping = false
	close(p.tasks)
}

func (p *ParallelImpl) Wait() {
	p.wg.Wait()
}
func (p *ParallelImpl) Run() {
	p.Start().Wait()
}

func New(max int) Parallel {
	if max <= 0 {
		max = runtime.NumCPU()
	}
	return &ParallelImpl{
		uuid:        uuid.NewV4(),
		maxRoutines: max,
		looping:     false,
		tasks:       make(chan Task, 1000),
	}
}
