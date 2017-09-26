package rocketmq

import (
	"github.com/golang/glog"
)

type CoGoEntity struct{
	entity interface{}
	resultChan chan interface{}
}

type GoCoRoutingPool struct {
	entityChan chan *CoGoEntity
	coRoutingCount int
	run func(entity interface{}) (interface{},error)
}

func NewGoCoRoutingPool(coRoutingCount int,queueSize int,run func(entity interface{}) (interface{},error)) (*GoCoRoutingPool, error) {
	pool:=new(GoCoRoutingPool)
	pool.coRoutingCount=coRoutingCount
	if queueSize<coRoutingCount{
		queueSize=coRoutingCount
	}
	pool.entityChan=make(chan *CoGoEntity,queueSize)
	pool.run=run
	glog.Infoln("successfully inited routingprod")
	return pool, nil
}

func (self *GoCoRoutingPool) Start() error {
	for w := 1; w <= self.coRoutingCount; w++ {
		go work(w,self.entityChan,self.run)
	}
	return nil
}

func (self *GoCoRoutingPool) Shutdown() {
}

func (self *GoCoRoutingPool) Do(entity interface{}) (result interface{},err error){
	coGoEntity:=new(CoGoEntity)
	coGoEntity.resultChan=make(chan interface{})
	coGoEntity.entity=entity
	self.entityChan<-coGoEntity
	res:=<-coGoEntity.resultChan
	return res, nil
	//select {
	//	case self.entityChan<-coGoEntity:
	//		break
	//	case <-time.After(60 * time.Second):
	//		glog.Errorln("err to send entity to channel timeout:60s")
	//		return nil, errors.New("err to send entity to channel timeout:60s")
	//}
	//select {
	//	case result:=<-coGoEntity.resultChan:
	//		return result, nil
	//	case <-time.After(60 * time.Second):
	//		glog.Errorln("invoke sync timeout:60s")
	//		return nil, errors.New("invoke sync timeout:60s")
	//}
}

func work(id int,entityChan chan *CoGoEntity,run func(entity interface{}) (interface{},error)) {
	for {
		entity := <- entityChan
		//fmt.Printf("worker:%d processing job:%v\n", id, entity)
		result,err:=run(entity.entity)
		if err!=nil{
			glog.Error(err)
		}
		entity.resultChan<-result
	}
}