package rocketmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type GetRouteInfoRequestHeader struct {
	topic string
}

func (self *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{\"topic\":\"")
	buf.WriteString(self.topic)
	buf.WriteString("\"}")
	return buf.Bytes(), nil
}

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int32
	TopicSynFlag   int32
}

type BrokerData struct {
	BrokerName  string
	BrokerAddrs map[string]string
	BrokerAddrsLock sync.RWMutex
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

type Config struct {
	Nameserver   string
	ClientIp     string
	InstanceName string
}

type MqClient struct {
	clientId           string
	conf               *Config
	//brokerAddrTable    map[string]map[string]string //map[brokerName]map[bokerId]addrs
	//brokerAddrTableLock sync.RWMutex
	brokerAddrTable    *sync.Map //map[brokerName]map[bokerId]addrs
	producerTable      *sync.Map
	topicRouteTable    *sync.Map
	remotingClient     RemotingClient
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable: new(sync.Map),//make(map[string]map[string]string),
		topicRouteTable: new(sync.Map),
	}
}

func (self *MqClient) findBrokerAddressInAdmin(brokerName string) (addr string, found, slave bool) {
	found = false
	slave = false
	bb, ok := self.brokerAddrTable.Load(brokerName)
	if ok {
		brokers:=bb.(map[string]string)
		for brokerId, addr := range brokers {
			if addr != "" {
				found = true
				if brokerId == "0" {
					slave = false
				} else {
					slave = true
				}
				return addr,found,slave
			}
		}
	}
	return
}

func (self *MqClient) findBrokerAddrByTopic(topic string) (addr string, ok bool) {
	tt, ok := self.topicRouteTable.Load(topic)
	if !ok {
		return "", ok
	}
	topicRouteData:=tt.(*TopicRouteData)
	brokers := topicRouteData.BrokerDatas
	if brokers != nil && len(brokers) > 0 {
		brokerData := brokers[0]
		if ok {
			brokerData.BrokerAddrsLock.RLock()
			addr, ok = brokerData.BrokerAddrs["0"]
			brokerData.BrokerAddrsLock.RUnlock()

			if ok {
				return
			}
			for _, addr = range brokerData.BrokerAddrs {
				return addr, ok
			}
		}
	}
	return
}

func (self *MqClient) getTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) (*TopicRouteData, error) {
	requestHeader := &GetRouteInfoRequestHeader{
		topic: topic,
	}

	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = GET_ROUTEINTO_BY_TOPIC
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79

	remotingCommand.ExtFields = requestHeader
	response, err := self.remotingClient.invokeSync(self.conf.Nameserver, remotingCommand, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code == SUCCESS {
		topicRouteData := new(TopicRouteData)
		bodyjson := strings.Replace(string(response.Body), ",0:", ",\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1)
		bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), topicRouteData)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		return topicRouteData, nil
	} else {
		return nil, errors.New(fmt.Sprintf("get topicRouteInfo from nameServer error[code:%d,topic:%s]", response.Code, topic))
	}

}

func (self *MqClient) updateProducerTopicRouteInfoFromNameServer() {
	self.producerTable.Range(func(key, value interface{}) bool{
		producer:=value.(*DefaultProducer)
		topicPublishInfoTable := producer.topicPublishInfoTable
		for topic := range topicPublishInfoTable {
			self.updateTopicRouteInfoFromNameServerByTopic(topic,false)
		}
		return true
	})
}

func (self *MqClient) updateTopicRouteInfoFromNameServerByTopic(topic string,isDefault bool) error {
	var topicRouteData *TopicRouteData
	var err error
	if isDefault{
		topicRouteData,err = self.getTopicRouteInfoFromNameServer(DEFAULTCRATETOPICKEY, 300*1000)
	}else{
		topicRouteData,err = self.getTopicRouteInfoFromNameServer(topic, 300*1000)
	}
	if err != nil {
		glog.Warning(err)
		//debug.PrintStack()
		return err
	}

	for _, bd := range topicRouteData.BrokerDatas {
		self.brokerAddrTable.Store(bd.BrokerName,bd.BrokerAddrs)
	}
	updateProducerTopic(topicRouteData, topic, self)
	self.topicRouteTable.Store(topic,topicRouteData)

	return nil
}

func updateProducerTopic(topicRouteData *TopicRouteData, topic string, self *MqClient) {
	mqList := buildmqList(topicRouteData, topic)
	topicinfo := &TopicPublishInfo{
		false,
		true,
		mqList,
		1,
	}
	self.producerTable.Range(func(key, value interface{}) bool {
		producer:=value.(*DefaultProducer)
		producer.UpdateTopicPublishInfo(topic, topicinfo)
		return true
	})
}
func buildmqList(topicRouteData *TopicRouteData, topic string) []*MessageQueue {
	mqList := make([]*MessageQueue, 0)
	for _, queueData := range topicRouteData.QueueDatas {
		var i int32
		for i = 0; i < queueData.ReadQueueNums; i++ {
			mq := &MessageQueue{
				topic:      topic,
				brokerName: queueData.BrokerName,
				queueId:    i,
			}

			mqList = append(mqList, mq)
		}
	}
	return mqList
}

type HeartbeatData struct {
	ClientId        string
}

func (self *MqClient) startScheduledTask() {
	go func() {
		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		for {
			<-updateTopicRouteTimer.C
			self.updateProducerTopicRouteInfoFromNameServer()
			updateTopicRouteTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		scanResponseRouteTimer := time.NewTimer(30 * time.Second)
		for {
			<-scanResponseRouteTimer.C
			self.remotingClient.ScanResponseTable()
			scanResponseRouteTimer.Reset(30 * time.Second)
		}
	}()
}

func (self *MqClient) startProducer() {
	self.startScheduledTask()
}
