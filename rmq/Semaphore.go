package rmq
import (
	"time"
)

type Semaphore struct {
	sem chan int
	length int
	timeout time.Duration
}

func NewSemaphore(length int,timeout time.Duration) *Semaphore{
	semaphore:=new(Semaphore)
	semaphore.length=length
	semaphore.timeout=timeout
	semaphore.sem=make(chan int,length)
	return semaphore
}

func (self *Semaphore) tryAcquire() bool {
	select {
	case self.sem <- 1:
		return true
	case <-time.After(self.timeout):
		return false
	}
}

func (self *Semaphore) release() bool {
	select {
	case <-self.sem:
		return true
	case <-time.After(self.timeout):
		return false
	}
}


