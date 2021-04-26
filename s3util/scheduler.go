package s3util

type task struct {
	taskFunc func() error
	done     chan struct{}
	errs     chan error
}

type BatchTasks struct {
	tasks []*task
}

func NewBatchTasks() *BatchTasks {
	return &BatchTasks{}
}

func (b *BatchTasks) AppendTask(f func() error) {
	b.tasks = append(b.tasks, &task{
		taskFunc: f,
	})
}

type scheduler struct {
	tasks   chan *task
	workers chan struct{}
}

func newScheduler(numWorkers int) *scheduler {
	return &scheduler{
		tasks:   make(chan *task),
		workers: make(chan struct{}, numWorkers),
	}
}

func (s *scheduler) BatchSchedule(b *BatchTasks) error {
	done := make(chan struct{}, len(b.tasks))
	errs := make(chan error, len(b.tasks))
	for i := range b.tasks {
		t := b.tasks[i]
		t.done = done
		t.errs = errs
		select {
		case err := <-errs:
			return err
		case s.tasks <- t:
		case s.workers <- struct{}{}:
			go s.worker(t)
		}
	}
	for i := 0; i < len(b.tasks); i++ {
		select {
		case err := <-errs:
			return err
		case <-done:
		}
	}
	if len(errs) > 0 {
		err := <-errs
		return err
	}
	return nil
}

func (w *scheduler) worker(t *task) {
	for {
		err := t.taskFunc()
		if err != nil {
			t.errs <- err
		}
		t.done <- struct{}{}
		select {
		case t = <-w.tasks:
		default:
			<-w.workers
			return
		}
	}
}
