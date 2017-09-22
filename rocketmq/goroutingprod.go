package rocketmq

import (
	"fmt"
	//"time"
	"sync"
	"sync/atomic"
	"github.com/golang/glog"
	"time"
)

type MsgRes struct {
	msg *Message
	sed int64
}


type GoroutingProd struct {
	producer     Producer
	msgChan   chan *MsgRes
	coRoutingCount int
	mapResult map[int64]*SendResult
	resultLock sync.RWMutex
	seed int64
}

func NewRoutingProducer(producer Producer,coRoutingCount int) (Producer, error) {
	prod:=new(GoroutingProd)
	prod.producer=producer
	prod.coRoutingCount=coRoutingCount
	var chanCount int
	if coRoutingCount>=50000{
		chanCount=50000
	}  else {
		chanCount=coRoutingCount
	}
	prod.msgChan=make(chan *MsgRes,chanCount)
	prod.mapResult=make(map[int64]*SendResult)
	prod.seed=0
	fmt.Printf("successfully inited routingprod\n")
	return prod, nil
}

func (self *GoroutingProd) Start() error {
	var prod Producer=self.producer
	prod.Start()
	if self.coRoutingCount<=0{
		self.coRoutingCount=10000
	}
	for w := 1; w <= self.coRoutingCount; w++ {
		go self.worker(prod,w, self.msgChan)
	}
	return nil
}

func (self *GoroutingProd) Shutdown() {
	var prod Producer=self.producer
	prod.Shutdown()
}

func (self *GoroutingProd) FetchPublishMessageQueues(topic string) MessageQueues {
	var prod Producer=self.producer
	return prod.FetchPublishMessageQueues(topic)
}

func (self *GoroutingProd) Send(msg *Message) (*SendResult, error) {
	msgRes:=new(MsgRes)
	self.seed=atomic.AddInt64(&self.seed,1)
	msgRes.msg=msg
	msgRes.sed=self.seed
	self.msgChan<-msgRes

	sendResult:=self.awaitSendResult(msgRes)
	return sendResult,nil
}

func (self *GoroutingProd) worker(producer Producer,id int,chanMsg <- chan *MsgRes) {
	for {
		msg := <- chanMsg
		//fmt.Println("worker", id, "processing job", string(msgRes.msg.Body))
		result,_:=producer.Send(msg.msg)
		self.resultLock.Lock()
		self.mapResult[msg.sed]=result
		self.resultLock.Unlock()
	}
}

func (self *GoroutingProd) awaitSendResult(msgRes *MsgRes) *SendResult{
	var result *SendResult
	for i:=0;i<1000000000;i++ {
		self.resultLock.RLock()
		result=self.mapResult[msgRes.sed]
		self.resultLock.RUnlock()
		if result!=nil{
			return result
		}
		time.Sleep(time.Nanosecond)
	}
	glog.Errorf("errorSynctimeout\n")
	return nil
}