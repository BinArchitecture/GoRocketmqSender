package rocketmq

import (
	"github.com/golang/glog"
)

type GoRoutingPoolProd struct {
	producer       Producer
	coRoutingCount int
	goRoutingPool  *GoCoRoutingPool
}

func NewRoutingPoolProducer(coRoutingCount int, prodGroup string, nameAddr string, prodInstance string) (Producer, error) {
	producer, err := NewDefaultProducer(prodGroup, nameAddr, prodInstance)
	if err != nil {
		panic(err)
	}
	prod := new(GoRoutingPoolProd)
	prod.producer = producer
	var chanCount int
	if coRoutingCount >= 50000 {
		chanCount = 50000
	} else {
		chanCount = coRoutingCount
	}
	prod.coRoutingCount = chanCount
	run := func(entity interface{}) (interface{}, error) {
		msg := entity.(*Message)
		result, err := prod.producer.Send(msg)
		if err != nil {
			glog.Error(err)
		}
		return result, err
	}
	prod.goRoutingPool, _ = NewGoCoRoutingPool(prod.coRoutingCount, run)
	glog.Infoln("successfully inited GoRoutingPoolProd")
	return prod, nil
}

func (self *GoRoutingPoolProd) Start() error {
	var prod Producer = self.producer
	prod.Start()
	self.goRoutingPool.Start()
	return nil
}

func (self *GoRoutingPoolProd) Shutdown() {
	var prod Producer = self.producer
	prod.Shutdown()
}

func (self *GoRoutingPoolProd) FetchPublishMessageQueues(topic string) MessageQueues {
	var prod Producer = self.producer
	return prod.FetchPublishMessageQueues(topic)
}

func (self *GoRoutingPoolProd) Send(msg *Message) (*SendResult, error) {
	result, err := self.goRoutingPool.Do(msg)
	if res, ok := result.(*SendResult); ok {
		return res, err
	}
	return nil, err
}
