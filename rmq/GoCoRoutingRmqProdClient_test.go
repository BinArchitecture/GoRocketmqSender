package rmq

import (
	"flag"
	"runtime"
	"fmt"
	"testing"
	"strconv"
)

func Benchmark_SendMsg(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	addr:="10.6.30.141:7912"
	size:=10000
	//wg := sync.WaitGroup{}
	//wg.Add(size)
	cc,_:=NewGoCoRoutingRmqProdClient(addr,5000,3000,3600,size/2)
	for j:=0;j<size;j++{
		props := make(map[string]string)
		props["bintest"] = "asd"
		d := strconv.Itoa(j)
		d = d + "大神哈哈bintest"
		body := []byte(d)
		//body := []byte("大神哈哈bintest")
		msg := &RmqMessage{"mqbintestodr", 0, props, body}
		rmresult,_:=cc.SendMsgOdrly(msg,777)
		if rmresult==nil{
			fmt.Errorf("rmresult send Fail\n")
			return
		}
		fmt.Printf("rmresult:%v\n",rmresult)
		fmt.Printf("msg:%v\n",string(msg.Body))
		if !rmresult.IsSendOK{
			fmt.Errorf("rmresult:%v send Fail\n",rmresult)
		}
		//go func(msg *RmqMessage) {
		//	defer wg.Done()
		//	rmresult,_:=cc.SendMsg(msg)
		//	if rmresult==nil{
		//		fmt.Errorf("rmresult send Fail\n")
		//		return
		//	}
		//	fmt.Printf("rmresult:%v\n",rmresult)
		//	if !rmresult.IsSendOK{
		//		fmt.Errorf("rmresult:%v send Fail\n",rmresult)
		//	}
		//}(msg)
	}
	//wg.Wait()
	cc.ShutDown()
}