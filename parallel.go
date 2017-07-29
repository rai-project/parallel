package parallel

import (
	"runtime"
	"runtime/debug"

	"github.com/rai-project/uuid"
)

type Parallel interface {
	Start() Parallel
	Stop()
	Add(Task)
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
}

func (parallel *ParallelImpl) loop() {
	defer func() {
		if r := recover(); r != nil {
			log.WithField("uuid", parallel.uuid).
				WithField("stack", debug.Stack()).
				WithField("recover", r).
				Error("panic")

			go parallel.loop()
		}
	}()

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

func New(max int) Parallel {
	if max == -1 {
		max = runtime.NumCPU()
	}
	return &ParallelImpl{
		uuid:        uuid.NewV4(),
		maxRoutines: max,
		tasks:       make(chan Task, 1000),
	}
}
