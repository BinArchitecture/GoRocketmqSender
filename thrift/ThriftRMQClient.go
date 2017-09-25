package main

import (
	"flag"
	"runtime"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/BinArchitecture/GoRocketmqSender/rmq"
	"github.com/golang/glog"
	"fmt"
	"time"
	"sync"
	"sync/atomic"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	addr:="10.6.30.109:7912"
	props := make(map[string]string)
	props["fuck"] = "asd"
	//d := strconv.Itoa(j)
	//d = d + "大神哈哈fuck"
	//body := []byte(d)
	body := []byte("大神哈哈fuck")
	msg := &rmq.RmqMessage{"mqfuck", 0, props, body}
	size:=30000
	clientPool,_:=buildClientPool(addr,size,3000)
	//client,_:=buildClient(addr)
	wg := sync.WaitGroup{}
	wg.Add(size)
	var errCount int32=0
	for j:=0;j<size;j++{
		go func() {
			defer wg.Done()
			ttport:=clientPool.Get()
			if ttport==nil{
				errCount=atomic.AddInt32(&errCount,1)
				return
			}
			pF := thrift.NewTBinaryProtocolFactoryDefault()
			client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
			rmresult,_:=client.Send(msg)
			fmt.Printf("rmresult:%v\n",rmresult)
			clientPool.Release(ttport)
		}()
	}
	wg.Wait()
	clientPool.Destory()
	fmt.Printf("errCount:%d",int(errCount))
	//client.Transport.Close()
	//for{
	//	time.Sleep(time.Second)
	//}
}

func buildClient(addr string) (*rmq.RmqThriftProdServiceClient,error){
	sock, err := thrift.NewTSocket(addr)
	if err != nil {
		glog.Errorf("thrift.NewTSocketTimeout(%s) error(%v)", addr, err)
		return nil,err
	}
	tF := thrift.NewTTransportFactory()
	pF := thrift.NewTBinaryProtocolFactoryDefault()
	ttport,_:=tF.GetTransport(sock)
	client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
	if err = client.Transport.Open(); err != nil {
		glog.Errorf("client.Transport.Open() error(%v)", err)
		return nil,err
	}
	return client,nil
}

func buildClientPool(addr string,poolSize int,minSize int) (*rmq.ThriftTransportPool,error){
	addrs:=make([]string,1)
	addrs[0]=addr
	return rmq.NewThriftTransportPool(10 * time.Second,1000,500,30,10*time.Second,addrs),nil
}
