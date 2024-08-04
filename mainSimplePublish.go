package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
)

func main() {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("imoocsimple")
	rabbitmq.PublishSimple("hello imooc")
	fmt.Println("发送成功")
}
