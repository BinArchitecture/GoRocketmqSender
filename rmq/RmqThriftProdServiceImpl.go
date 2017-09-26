package rmq

import (
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
	"github.com/golang/glog"
	"errors"
)

type RmqThriftProdServiceImpl struct {
	Producer rocketmq.Producer
}

func (self *RmqThriftProdServiceImpl) Start() error {
	var prod rocketmq.Producer=self.Producer
	prod.Start()
	return nil
}

func (self *RmqThriftProdServiceImpl) Send(msg *RmqMessage) (*RmqSendResult_,error) {
	msg_:=self.convertMsg(msg)
	var prod rocketmq.Producer=self.Producer
	sr,_:=prod.Send(msg_)
	result:= self.convertResult(sr)
	//fmt.Printf("result is:%v\n",result)
	return result,nil
}

func (self *RmqThriftProdServiceImpl) SendOrderly(msg *RmqMessage, orderKey int32) (r *RmqSendResult_, err error){
	if orderKey<0{
		glog.Errorln("orderKey<0 err:",msg,orderKey)
		return nil,errors.New("orderKey<0 error")
	}
	msg_:=self.convertMsg(msg)
	var prod rocketmq.Producer=self.Producer
	sr,_:=prod.SendOrderly(msg_,int(orderKey))
	result:= self.convertResult(sr)
	//fmt.Printf("result is:%v\n",result)
	return result,nil
}

func (self *RmqThriftProdServiceImpl) convertResult(sr *rocketmq.SendResult) (*RmqSendResult_) {
	rs:=new(RmqSendResult_)
	rs.QueueId=sr.QueueId
	rs.IsSendOK=sr.IsSendOK
	rs.MsgId=sr.MsgId
	rs.QueueOffset=sr.QueueOffset
	return rs
}

func (self *RmqThriftProdServiceImpl) convertMsg(msg *RmqMessage) (*rocketmq.Message) {
	msg_:=new(rocketmq.Message)
	msg_.Body=msg.Body
	msg_.Properties=msg.Properties
	msg_.Flag=msg.Flag
	msg_.Topic=msg.Topic
	return msg_
}

