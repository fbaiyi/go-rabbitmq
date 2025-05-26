package main

import (
	"fmt"
	gorabbitmq "github.com/fbaiyi/go-rabbitmq/v2"
	"sync"
	"time"
)

func main() {
	config := &gorabbitmq.Config{
		Host:     "127.0.0.1",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
		Vhost:    "",
	}
	// 注意 队列是否持久化.false:队列在内存中,服务器挂掉后,队列就没了;true:服务器重启后,队列将会重新生成.注意:只是队列持久化,不代表队列中的消息持久化!!!!
	// 已存在的队列 查看 Features参数是否为持久化（D），不存在的队列按需设置是否持久化
	mq, err := gorabbitmq.New(config, "delay_message", "delay", "delay_message", 0, 1, true, true)
	if err != nil {
		panic("err" + err.Error())
	}
	// 链接或重连成功后执行
	for {
		select {
		case <-mq.ConnSuccess:
			go func() {
				err = amqpDelayHandler(mq, 3)
				if err != nil {
					panic(err)
				}
			}()
		}
	}
}

// amqphandler 消息队列处理
func amqpDelayHandler(mq *gorabbitmq.RabbitMQ, consumerNum int) error {
	var wg sync.WaitGroup
	cherrors := make(chan error)
	wg.Add(consumerNum)
	for i := 0; i < consumerNum; i++ {
		fmt.Printf("正在开启消费者：第 %d 个\n", i+1)
		go func() {
			defer wg.Done()
			deliveries, err := mq.Consume()
			if err != nil {
				cherrors <- err
			}
			for d := range deliveries {
				// 消费者逻辑 to do
				fmt.Printf("%sgot %dbyte delivery: [%v] %s %q\n", time.Now().Format("2006-01-02 15:04:05"), len(d.Body), d.DeliveryTag, d.Exchange, d.Body)
				d.Ack(false)
			}
		}()
	}
	select {
	case err := <-cherrors:
		close(cherrors)
		fmt.Printf("Consumer failed: %s\n", err)
		return err
	}
	wg.Wait()
	return nil
}
