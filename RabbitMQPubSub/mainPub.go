package main

import (
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

func main() {
	// 1. 创建连接
	rabbitmq := RabbitMQ.NewRabbitMQPubSub("" + "newProduct")
	for i := 0; i < 100; i++ {
		rabbitmq.PublishPub("订阅模式生产第" + strconv.Itoa(i) + "条" + "消息")
		time.Sleep(1 * time.Second)
	}
}
