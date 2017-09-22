package rocketmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"strconv"
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
	brokerAddrTable    map[string]map[string]string //map[brokerName]map[bokerId]addrs
	brokerAddrTableLock sync.RWMutex
	producerTable      map[string]*DefaultProducer
	producerTableLock *sync.RWMutex
	topicRouteTable    map[string]*TopicRouteData
	topicRouteTableLock sync.RWMutex
	remotingClient     RemotingClient
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable: make(map[string]map[string]string),
		topicRouteTable: make(map[string]*TopicRouteData),
	}
}
func (self *MqClient) findBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) (brokerAddr string, slave bool, found bool) {
	slave = false
	found = false
	self.brokerAddrTableLock.RLock()
	brokerMap, ok := self.brokerAddrTable[brokerName]
	self.brokerAddrTableLock.RUnlock()
	if ok {
		brokerAddr, ok = brokerMap[strconv.FormatInt(brokerId, 10)]
		slave = (brokerId != 0)
		found = ok

		if !found && !onlyThisBroker {
			var id string
			for id, brokerAddr = range brokerMap {
				slave = (id != "0")
				found = true
				break
			}
		}
	}

	return
}

func (self *MqClient) findBrokerAddressInAdmin(brokerName string) (addr string, found, slave bool) {
	found = false
	slave = false
	self.brokerAddrTableLock.RLock()
	brokers, ok := self.brokerAddrTable[brokerName]
	self.brokerAddrTableLock.RUnlock()
	if ok {
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
	self.topicRouteTableLock.RLock()
	topicRouteData, ok := self.topicRouteTable[topic]
	self.topicRouteTableLock.RUnlock()
	if !ok {
		return "", ok
	}

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
	for _, producer := range self.producerTable {
		topicPublishInfoTable := producer.topicPublishInfoTable
		for topic := range topicPublishInfoTable {
			self.updateTopicRouteInfoFromNameServerByTopic(topic,false)
		}
	}
}

func (self *MqClient) updateTopicRouteInfoFromNameServerByTopic(topic string,isConsume bool) error {

	topicRouteData, err := self.getTopicRouteInfoFromNameServer(topic, 3000*1000)
	if err != nil {
		glog.Error(err)
		return err
	}

	for _, bd := range topicRouteData.BrokerDatas {
		self.brokerAddrTableLock.Lock()
		self.brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs
		self.brokerAddrTableLock.Unlock()
	}
	updateProducerTopic(topicRouteData, topic, self)

	self.topicRouteTableLock.Lock()
	self.topicRouteTable[topic] = topicRouteData
	self.topicRouteTableLock.Unlock()

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
	for _, producer := range self.producerTable {
		producer.UpdateTopicPublishInfo(topic, topicinfo)
	}
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

func (self *MqClient) startScheduledTask(isPull bool) {
	go func() {
		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		for {
			<-updateTopicRouteTimer.C
			self.updateProducerTopicRouteInfoFromNameServer()
			updateTopicRouteTimer.Reset(5 * time.Second)
		}
	}()
}

func (self *MqClient) startProducer() {
	self.startScheduledTask(false)
}
