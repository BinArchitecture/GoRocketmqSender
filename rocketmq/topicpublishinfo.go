package rocketmq

import (
	"sync/atomic"
)

type TopicPublishInfo struct {
	OrderTopic    bool
	HaveTopicRouterInfo bool
	MessageQueueList MessageQueues
	SendWhichQueue   int32
}

func (self *TopicPublishInfo) SelectOneMessageQueue(orderKey int) (*MessageQueue, error) {
	if orderKey >0 {
		pos := orderKey % self.MessageQueueList.Len()
		mq := self.MessageQueueList[pos]
		return mq,nil
	}else {
		index := atomic.AddInt32(&self.SendWhichQueue,1)
		pos := int(index) % self.MessageQueueList.Len()
		return self.MessageQueueList[pos],nil
	}
}