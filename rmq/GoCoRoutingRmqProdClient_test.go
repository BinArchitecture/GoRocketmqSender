package rmq

import (
	"flag"
	"runtime"
	"fmt"
	"sync"
	"testing"
)

func Benchmark_SendMsg(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	addr:="10.6.30.109:7912"
	props := make(map[string]string)
	props["fuck"] = "asd"
	//d := strconv.Itoa(j)
	//d = d + "大神哈哈fuck"
	//body := []byte(d)
	body := []byte("大神哈哈fuck")
	msg := &RmqMessage{"mqfuck", 0, props, body}
	size:=100000
	//client,_:=buildClient(addr)
	wg := sync.WaitGroup{}
	wg.Add(size)
	cc,_:=NewGoCoRoutingRmqProdClient(addr,5000,3000,3600,size/2)
	for j:=0;j<size;j++{
		go func() {
			defer wg.Done()
			rmresult,_:=cc.SendMsg(msg)
			if rmresult==nil{
				fmt.Errorf("rmresult send Fail\n")
				return
			}
			fmt.Printf("rmresult:%v\n",rmresult)
			if !rmresult.IsSendOK{
				fmt.Errorf("rmresult:%v send Fail\n",rmresult)
			}
		}()
	}
	wg.Wait()
	cc.ShutDown()
}

//func buildClient(addr string) (*rmq.RmqThriftProdServiceClient,error){
//	sock, err := thrift.NewTSocket(addr)
//	if err != nil {
//		glog.Errorf("thrift.NewTSocketTimeout(%s) error(%v)", addr, err)
//		return nil,err
//	}
//	tF := thrift.NewTTransportFactory()
//	pF := thrift.NewTBinaryProtocolFactoryDefault()
//	ttport,_:=tF.GetTransport(sock)
//	client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
//	if err = client.Transport.Open(); err != nil {
//		glog.Errorf("client.Transport.Open() error(%v)", err)
//		return nil,err
//	}
//	return client,nil
//}