package main

import (
	"flag"
	"runtime"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/BinArchitecture/GoRocketmqSender/rmq"
	"github.com/golang/glog"
	"fmt"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	addr:="10.6.30.109:7912"
	sock, err := thrift.NewTSocket(addr)
	if err != nil {
		glog.Errorf("thrift.NewTSocketTimeout(%s) error(%v)", addr, err)
		return
	}
	tF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	pF := thrift.NewTBinaryProtocolFactoryDefault()
	ttport,_:=tF.GetTransport(sock)
	client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
	if err = client.Transport.Open(); err != nil {
		glog.Errorf("client.Transport.Open() error(%v)", err)
		return
	}
	props := make(map[string]string)
	props["fuck"] = "asd"
	//d := strconv.Itoa(j)
	//d = d + "大神哈哈fuck"
	//body := []byte(d)
	body := []byte("大神哈哈fuck")
	msg := &rmq.RmqMessage{"mqfuck", 0, props, body}
	for i:=0;i<100000;i++{
			rmresult,_:=client.Send(msg)
			fmt.Printf("rmresult:%v\n",rmresult)
	}
}
