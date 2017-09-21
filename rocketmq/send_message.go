package rocketmq

type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int32  `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int32  `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int32  `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
}

type SendResult struct {
	MsgId       string
	QueueId     string
	QueueOffset string
	IsSendOK    bool
}

func BuildSendResult(cmd *RemotingCommand) *SendResult{
	result:=new(SendResult)
	mm,ok:=cmd.ExtFields.(map[string]interface{})
	if ok{
		result.QueueId=mm["queueId"].(string)
		result.MsgId=mm["msgId"].(string)
		result.QueueOffset=mm["queueOffset"].(string)
	}else{
		mc:=cmd.ExtFields.(map[string]string)
		result.QueueId=mc["queueId"]
		result.MsgId=mc["msgId"]
		result.QueueOffset=mc["queueOffset"]
	}
	result.IsSendOK=cmd.Code==0
	return result
}