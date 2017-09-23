package rmq

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/golang/glog"
	"time"
	"sync"
)

type ThriftTransportPool struct {
	semaphore           *Semaphore
	pool                []*TransfortWrapper
	poolSize            int
	minSize             int
	maxIdleSecond       int
	checkInvervalSecond time.Duration
	addrs               []string
	allowCheck          bool
	mutlock             sync.RWMutex
}

func NewThriftTransportPool(semtimeout time.Duration, poolSize int, minSize int, maxIdleSecond int, checkInvervalSecond time.Duration, addrs []string) *ThriftTransportPool {
	pool := new(ThriftTransportPool)
	pool.poolSize = poolSize
	pool.minSize = minSize
	pool.maxIdleSecond = maxIdleSecond
	pool.checkInvervalSecond = checkInvervalSecond
	pool.addrs = addrs
	pool.semaphore = NewSemaphore(poolSize, semtimeout)
	pool.pool = make([]*TransfortWrapper, poolSize)
	pool.allowCheck = true
	for i := 0; i < poolSize; i++ {
		j := i % len(addrs)
		sock, err := thrift.NewTSocket(addrs[j])
		if err != nil {
			glog.Errorf("thrift.NewTSocketTimeout(%s) error(%v)", addrs[j], err)
			panic(err)
		}
		pool.pool[i] = NewTransfortWrapper(sock, addrs[j], i < minSize)
	}
	go func() {
		poolCheckerTimer := time.NewTimer(checkInvervalSecond)
		for pool.allowCheck {
			<-poolCheckerTimer.C
			doCheck(poolSize, pool, maxIdleSecond, minSize)
			poolCheckerTimer.Reset(checkInvervalSecond)
		}
	}()
	return pool
}

func doCheck(poolSize int, pool *ThriftTransportPool, maxIdleSecond int, minSize int) {
	glog.Infoln("开始检测空闲连接...")
	for i := 1; i < poolSize; i++ {
		if pool.pool[i].isAvailable() /*&& pool.pool[i].lastUseTime != nil*/ {
			idleTime := time.Now().Unix() - pool.pool[i].lastUseTime.Unix()
			if int(idleTime) > maxIdleSecond {
				if pool.getActiveCount() > minSize {
					pool.pool[i].transport.Close()
					pool.pool[i].isBusy = false
					glog.Infoln("%s超过空闲时间阀值被断开！", pool.pool[i].addr)
				}
			}
		}
	}
	glog.Infoln("当前活动连接数：%d", pool.getActiveCount())
}

func (self *ThriftTransportPool) getActiveCount() int {
	result := 0
	for i := 0; i < len(self.pool); i++ {
		if !self.pool[i].isDead && self.pool[i].transport.IsOpen() {
			result += 1
		}
	}
	return result
}

func (self *ThriftTransportPool) Get() thrift.TTransport {
	if self.semaphore.tryAcquire() {
		self.mutlock.Lock()
		defer self.mutlock.Unlock()
		for i := 0; i < len(self.pool); i++ {
			if self.pool[i].isAvailable() {
				self.pool[i].isBusy = true
				self.pool[i].lastUseTime = time.Now()
				return self.pool[i].transport
			}
		}

		//尝试激活更多连接
		var pp *TransfortWrapper
		defer func() {
			r := recover()
			if err, ok := r.(error); ok {
				if pp != nil {
					glog.Errorf("%s error:%s", pp.addr, err.Error())
					pp.isDead = true
				}
			}
		}()
		for i := 0; i < len(self.pool); i++ {
			pp = self.pool[i]
			if !pp.isBusy && !pp.isDead && !pp.transport.IsOpen() {
				pp.transport.Open()
				pp.isBusy = true
				pp.lastUseTime = time.Now()
				return pp.transport
			}
		}
	}
	glog.Errorf("all client is too busy")
	return nil
}

func (self *ThriftTransportPool) Release(client thrift.TTransport) {
	released := false
	self.mutlock.Lock()
	for i := 0; i < len(self.pool); i++ {
		if client == self.pool[i].transport && self.pool[i].isBusy {
			self.pool[i].isBusy = false
			released = true
			break
		}
	}
	self.mutlock.Unlock()
	if released {
		self.semaphore.release()
	}
}

func (self *ThriftTransportPool) Destory() {
	if self.pool != nil {
		for i := 0; i < len(self.pool); i++ {
			self.pool[i].transport.Close()
		}
	}
	self.allowCheck = false
	glog.Infoln("连接池被销毁！")
}

type TransfortWrapper struct {
	transport   thrift.TTransport
	isBusy      bool
	isDead      bool
	lastUseTime time.Time
	addr        string
}

func NewTransfortWrapper(transport thrift.TTransport, addr string, isOpen bool) *TransfortWrapper {
	wrapper := new(TransfortWrapper)
	wrapper.lastUseTime = time.Now()
	wrapper.transport = transport
	wrapper.addr = addr
	if isOpen {
		transport.Open()
		defer func() {
			r := recover()
			if err, ok := r.(error); ok {
				glog.Errorf("%s error:%s", addr, err.Error())
				wrapper.isDead = true
			}
		}()
	}
	return wrapper
}

func (self *TransfortWrapper) isAvailable() bool {
	return !self.isBusy && !self.isDead && self.transport.IsOpen()
}
