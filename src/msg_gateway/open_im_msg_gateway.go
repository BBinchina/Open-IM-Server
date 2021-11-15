package main

import (
	"Open_IM/src/msg_gateway/gate"
	"flag"
	"sync"
)
// 消息网关，采用websocket协议接入，当消息写入mq即表示发送成功
func main() {
	rpcPort := flag.Int("rpc_port", 10500, "rpc listening port")
	wsPort := flag.Int("ws_port", 10800, "ws listening port")
	flag.Parse()
	//相当于java中的CountDownLatch，用于等待线程的完成,这里等待 1个协程
	var wg sync.WaitGroup
	wg.Add(1)
	gate.Init(*rpcPort, *wsPort)
	gate.Run()
	wg.Wait()
}
