package rocketmq

import (
	"errors"
	"github.com/golang/glog"
	"time"
)

type MsgRes struct {
	msg *Message
	chanResult chan *SendResult
}


type GoroutingProd struct {
	producer     Producer
	msgChan   chan *MsgRes
	coRoutingCount int
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
	//fmt.Printf("successfully inited routingprod\n")
	glog.Infoln("successfully inited routingprod")
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
	msgRes.chanResult=make(chan *SendResult)
	msgRes.msg=msg
	self.msgChan<-msgRes
	select {
	case result:=<-msgRes.chanResult:
		return result, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke sync timeout")
	}
}

func (self *GoroutingProd) worker(producer Producer,id int,chanMsg <- chan *MsgRes) {
	for {
		msg := <- chanMsg
		//fmt.Printf("worker:%d processing job:%s\n", id, string(msg.msg.Body))
		result,err:=producer.Send(msg.msg)
		if err!=nil{
			glog.Error(err)
		}
		msg.chanResult<-result
	}
}