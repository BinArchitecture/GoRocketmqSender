package rocketmq

import (
	"sync/atomic"
	"github.com/golang/glog"
)

type TopicPublishInfo struct {
	OrderTopic    bool
	HaveTopicRouterInfo bool
	MessageQueueList MessageQueues
	SendWhichQueue   int32
}

func (self *TopicPublishInfo) SelectOneMessageQueue(lastBrokerName string) (*MessageQueue, error) {
	if lastBrokerName != "" {
		index := atomic.AddInt32(&self.SendWhichQueue,1)
		for _,v := range self.MessageQueueList {
			glog.Info(v)
		   pos := int(index) % self.MessageQueueList.Len()
		   mq := self.MessageQueueList[pos]
		if mq.brokerName!=lastBrokerName {
		return mq,nil
		}
		}
		return self.MessageQueueList[0],nil
	}else {
		index := atomic.AddInt32(&self.SendWhichQueue,1)
		pos := int(index) % self.MessageQueueList.Len()
		return self.MessageQueueList[pos],nil
	}
}