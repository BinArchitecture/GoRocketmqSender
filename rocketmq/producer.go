package rocketmq

import (
	"net"
	"time"
	"sync/atomic"
	"os"
	"strconv"
	"sync"
	"fmt"
)

const (
	DEFAULTCRATETOPICKEY  = "TBW102"
)

type Producer interface {
	//Admin
	Start() error
	Shutdown()
	FetchPublishMessageQueues(topic string) MessageQueues
	Send(msg *Message) (*SendResult,error)
	SendOrderly(msg *Message,orderKey int) (*SendResult,error)
}

type DefaultProducer struct {
	producerGroup             string
	namesrvAddr               string
	prodInstanceName          string
	topicPublishInfoTable     map[string]*TopicPublishInfo
	topicPublishInfoTableLock *sync.RWMutex
	sendMsgTimeout            int
	defaultTopicQueueNums     int
	brokers                   map[string]net.Conn
	remotingClient            RemotingClient
	mqClient                  *MqClient
}

func NewDefaultProducer(producerGroup string, namesrvAddr string, prodInstanceName string) (Producer, error) {
	conf := &Config{
		Nameserver:   namesrvAddr,
		InstanceName: prodInstanceName,
	}
	if conf.ClientIp == "" {
		var DEFAULT_IP = GetLocalIp4()
		conf.ClientIp = DEFAULT_IP
	}

	remotingClient := NewDefaultRemotingClient()
	mqClient := NewMqClient()
	producer :=new(DefaultProducer)
	producer.producerGroup=producerGroup
	producer.namesrvAddr=namesrvAddr
	producer.prodInstanceName=prodInstanceName
	producer.topicPublishInfoTable =make(map[string]*TopicPublishInfo)
	producer.sendMsgTimeout=3000
	producer.defaultTopicQueueNums=4
	producer.brokers=make(map[string]net.Conn)
	producer.remotingClient=remotingClient
	producer.mqClient=mqClient
	producer.topicPublishInfoTableLock=new(sync.RWMutex)
	producer.topicPublishInfoTable[DEFAULTCRATETOPICKEY]=new(TopicPublishInfo)
	mqClient.remotingClient = remotingClient
	mqClient.producerTable=make(map[string]*DefaultProducer)
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.producerTableLock=new(sync.RWMutex)
	return producer, nil
}

func (self *DefaultProducer) Start() error {
	self.mqClient.startProducer()
	return nil
}

func (self *DefaultProducer) Shutdown() {
}

func (self *DefaultProducer) FetchPublishMessageQueues(topic string) MessageQueues{
	return nil
}

func (self *DefaultProducer) SendOrderly(msg *Message,orderKey int) (*SendResult,error){
	return self.sendmm(msg,orderKey)
}


func (self *DefaultProducer) Send(msg *Message) (*SendResult,error){
	return self.sendmm(msg,-1)
}

func (self *DefaultProducer) sendmm(msg *Message,orderKey int) (*SendResult,error){
	//maxTimeout := self.sendMsgTimeout+ 10000000
	//beginTimestamp := time.Now()
	// endTimestamp := beginTimestamp
	info,err := self.tryToFindTopicPublishInfo(msg.Topic)
	var respcmd *RemotingCommand
	if err!=nil{
		return nil,err
	}
	//fmt.Printf("info:%b",info==nil)
	if info != nil {
		mq,_:=info.SelectOneMessageQueue(orderKey)
		respcmd,err=self.sendKernel(msg,mq,true,nil)
		if err!=nil{
			fmt.Errorf("mqSendErr:%s",err.Error())
		}
	}
	if respcmd!=nil{
		return BuildSendResult(respcmd),err
	}
	return nil,err
}

func (self *DefaultProducer) sendKernel(msg *Message,mq *MessageQueue,isSync bool,invoke InvokeCallback) (*RemotingCommand,error) {
	brokerAddr,ok,_:=self.mqClient.findBrokerAddressInAdmin(mq.brokerName)
	if !ok || brokerAddr==""{
		self.tryToFindTopicPublishInfo(mq.topic)
		brokerAddr,_,_=self.mqClient.findBrokerAddressInAdmin(mq.brokerName)
	}
	prevBody := msg.Body
	prodreqheader:=new(SendMessageRequestHeader)
	prodreqheader.ProducerGroup=self.producerGroup
	prodreqheader.Topic=msg.Topic
	prodreqheader.DefaultTopic=DEFAULTCRATETOPICKEY
	prodreqheader.DefaultTopicQueueNums=int32(self.defaultTopicQueueNums)
	prodreqheader.QueueId=mq.queueId
	prodreqheader.Flag=msg.Flag
	prodreqheader.Properties=MessageProperties2String(msg)
	prodreqheader.BornTimestamp=time.Now().Unix()
	prodreqheader.ReconsumeTimes=0
	prodreqheader.SysFlag=0
	prodreqheader.UnitMode=false

	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = SEND_MESSAGE
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79
	remotingCommand.ExtFields = prodreqheader
	remotingCommand.Body=prevBody

	if isSync{
		respcmd,err:=self.remotingClient.invokeSync(brokerAddr,remotingCommand,int64(self.sendMsgTimeout+1000))
		if err==nil{
			return respcmd,nil
		}
		fmt.Errorf("prodSenderr:%s\n",err.Error())
		return nil,err
	}else{
		err:=self.remotingClient.invokeAsync(brokerAddr,remotingCommand,int64(self.sendMsgTimeout+1000),invoke)
		return nil,err
	}

}

func (self *DefaultProducer) tryToFindTopicPublishInfo(topic string) (*TopicPublishInfo,error){
	self.mqClient.producerTableLock.RLock()
	_,oo:=self.mqClient.producerTable[topic]
	self.mqClient.producerTableLock.RUnlock()
	if !oo {
		self.mqClient.producerTableLock.Lock()
		self.mqClient.producerTable[topic]=self
		self.mqClient.producerTableLock.Unlock()
	}
	var err error
	self.topicPublishInfoTableLock.RLock()
	info,ok :=self.topicPublishInfoTable[topic]
	self.topicPublishInfoTableLock.RUnlock()
	if !ok{
		self.topicPublishInfoTableLock.Lock()
		self.topicPublishInfoTable[topic]=new(TopicPublishInfo)
		err=self.mqClient.updateTopicRouteInfoFromNameServerByTopic(topic,false)
		self.topicPublishInfoTableLock.Unlock()
		self.topicPublishInfoTableLock.RLock()
		info=self.topicPublishInfoTable[topic]
		self.topicPublishInfoTableLock.RUnlock()
	}
	if info.HaveTopicRouterInfo{
		return info,nil
	}else{
		err=self.mqClient.updateTopicRouteInfoFromNameServerByTopic(topic,true)
		if err!=nil{
			return nil,err
		}
		self.topicPublishInfoTableLock.RLock()
		info=self.topicPublishInfoTable[topic]
		self.topicPublishInfoTableLock.RUnlock()
		return info,nil
	}
}

func (self *DefaultProducer) UpdateTopicPublishInfo(topic string, info *TopicPublishInfo) {
	if info != nil{
		self.topicPublishInfoTable[topic]=info
	}
}

