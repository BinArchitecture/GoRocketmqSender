package rmq

import (
	"github.com/golang/glog"
	"time"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"fmt"
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
)

type GoRoutingRmqMsg struct {
	odrKey int
	rmqMsg *RmqMessage
}

type GoCoRoutingRmqProdClient struct{
	thriftClientPool *ThriftTransportPool
	coRoutingPool *rocketmq.GoCoRoutingPool
}

func NewGoCoRoutingRmqProdClient(addr string,poolSize int,minSize int,coRotingSize int,queueSize int) (*GoCoRoutingRmqProdClient, error) {
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
	if queueSize<coRotingSize{
		queueSize=coRotingSize
	}
	cc.thriftClientPool=buildClientPool(addr,poolSize,minSize)
	run := func(entity interface{}) (interface{}, error) {
		msg := entity.(*GoRoutingRmqMsg)
		ttport:=cc.thriftClientPool.Get()
		if ttport==nil || !ttport.IsOpen(){
			return nil,errors.New("can't get client")
		}
		pF := thrift.NewTBinaryProtocolFactoryDefault()
		client := NewRmqThriftProdServiceClientFactory(ttport, pF)
		var result *RmqSendResult_
		var err error
		if msg.odrKey==-1{
			result, err = client.Send(msg.rmqMsg)
		}else{
			result, err = client.SendOrderly(msg.rmqMsg,int32(msg.odrKey))
		}
		if err != nil {
			glog.Error(err)
		}
		cc.thriftClientPool.Release(ttport)
		return result, err
	}
	if coRotingSize>poolSize || coRotingSize<minSize{
		coRotingSize=poolSize-500
	}
	if coRotingSize<=0{
		coRotingSize=100
	}
	cc.coRoutingPool,_ = rocketmq.NewGoCoRoutingPool(coRotingSize,queueSize,run)
	cc.coRoutingPool.Start()
	return cc,nil
}

func buildClientPool(addr string,poolSize int,minSize int) (*ThriftTransportPool){
	addrs:=make([]string,1)
	addrs[0]=addr
	return NewThriftTransportPool(30 * time.Second,poolSize,minSize,30,10*time.Second,addrs)
}

func (self *GoCoRoutingRmqProdClient) SendMsg(msg *RmqMessage) (*RmqSendResult_,error){
	mmsg:=new(GoRoutingRmqMsg)
	mmsg.rmqMsg=msg
	mmsg.odrKey=-1
	rmr,_:=self.coRoutingPool.Do(mmsg)
	if rmr==nil{
		fmt.Errorf("rmresult send Fail\n")
		return nil,errors.New("rmresult send Fail\n")
	}
	rmresult:=rmr.(*RmqSendResult_)
	return rmresult,nil
}

func (self *GoCoRoutingRmqProdClient) SendMsgOdrly(msg *RmqMessage,odrkey int) (*RmqSendResult_,error){
	mmsg:=new(GoRoutingRmqMsg)
	mmsg.rmqMsg=msg
	mmsg.odrKey=odrkey
	rmr,_:=self.coRoutingPool.Do(mmsg)
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
