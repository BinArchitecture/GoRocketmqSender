package rmq

import (
	"github.com/golang/glog"
	"time"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"fmt"
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
)

type GoCoRoutingRmqProdClient struct{
	thriftClientPool *ThriftTransportPool
	coRoutingPool *rocketmq.GoCoRoutingPool
}

func NewGoCoRoutingRmqProdClient(addr string,poolSize int,minSize int,coRotingSize int) (*GoCoRoutingRmqProdClient, error) {
	cc:=new(GoCoRoutingRmqProdClient)
	if poolSize<0 {
		poolSize=200
	}
	if minSize<0{
		minSize=100
	}
	if poolSize<minSize{
		poolSize=minSize
	}
	cc.thriftClientPool=buildClientPool(addr,poolSize,minSize)
	run := func(entity interface{}) (interface{}, error) {
		msg := entity.(*RmqMessage)
		ttport:=cc.thriftClientPool.Get()
		if ttport==nil || !ttport.IsOpen(){
			return nil,errors.New("can't get client")
		}
		pF := thrift.NewTBinaryProtocolFactoryDefault()
		client := NewRmqThriftProdServiceClientFactory(ttport, pF)
		result, err := client.Send(msg)
		if err != nil {
			glog.Error(err)
		}
		cc.thriftClientPool.Release(ttport)
		return result, err
	}
	if coRotingSize>poolSize || coRotingSize<minSize{
		coRotingSize=poolSize-1000
	}
	if coRotingSize<=0{
		coRotingSize=100
	}
	cc.coRoutingPool,_ = rocketmq.NewGoCoRoutingPool(coRotingSize, run)
	cc.coRoutingPool.Start()
	return cc,nil
}

func buildClientPool(addr string,poolSize int,minSize int) (*ThriftTransportPool){
	addrs:=make([]string,1)
	addrs[0]=addr
	return NewThriftTransportPool(30 * time.Second,poolSize,minSize,30,10*time.Second,addrs)
}

func (self *GoCoRoutingRmqProdClient) SendMsg(msg *RmqMessage) (*RmqSendResult_,error){
	rmr,_:=self.coRoutingPool.Do(msg)
	if rmr==nil{
		fmt.Errorf("rmresult send Fail\n")
		return nil,errors.New("rmresult send Fail\n")
	}
	rmresult:=rmr.(*RmqSendResult_)
	return rmresult,nil
}

func (self *GoCoRoutingRmqProdClient) ShutDown() {
	self.thriftClientPool.Destory()
	self.coRoutingPool.Shutdown()
	glog.Infoln("GoCoRoutingRmqProdClient shutdown")
}
