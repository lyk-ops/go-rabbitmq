package RabbitMQ

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

// url格式：amqp://用户名:密码@ip地址:端口号/虚拟主机名
const MQURL = "amqp://imoocuser:imoocuser@127.0.0.1:5672/imooc"

type RabbitMQ struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	QueueName string //队列名称
	Exchange  string //交换机
	Key       string //路由键
	Mqurl     string //连接地址
}

// NewRabbitMQ 创建RabbitMQ基础实例对象
func NewRabbitMQ(queueName string, exchange string, key string) *RabbitMQ {
	rabbitmq := &RabbitMQ{
		QueueName: queueName,
		Exchange:  exchange,
		Key:       key,
		Mqurl:     MQURL,
	}
	var err error
	//创建rabbitmq连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "Failed to connect to RabbitMQ")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	return rabbitmq
}

// Destory 断开channel和connection
func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

// failOnErr 错误处理
func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s:%s", err, message)
		panic(fmt.Sprintf("%s:%s", err, message))
	}
}

// NewRabbitMQSimple step1.simple模式下创建RabbitMQ实例对象
func NewRabbitMQSimple(queueName string) *RabbitMQ {
	return NewRabbitMQ(queueName, "", "") //默认使用default exchange
}

// NewRabbitMQPubSub 订阅模式创建RabbitMQ实例对象
func NewRabbitMQPubSub(exchangeName string) *RabbitMQ {
	return NewRabbitMQ("", "topic", "") //使用topic exchange
}

// NewRabbitMQRouting 路由模式创建RabbitMQ实例对象
func NewRabbitMQRouting(exchangeName string, routingKey string) *RabbitMQ {
	return NewRabbitMQ("", "direct", routingKey) //使用direct exchange
}

// NewRabbitMQTopic 主题模式创建RabbitMQ实例对象
func NewRabbitMQTopic(exchangeName string, routingKey string) *RabbitMQ {
	return NewRabbitMQ("", "topic", routingKey) //使用direct exchange
}

// PublishSimple step2.simpl模式下生产者发送消息
func (r *RabbitMQ) PublishSimple(message string) {
	// 1.声明队列,如果队列不存在则创建，如果存在跳过创建
	//保证在队列存在时，消息能发送到队列
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false, //是否持久化
		false, //是否自动删除
		false, //是否排他
		false, //是否等待
		nil,   //额外参数
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue:%s", err)
	}
	// 2.发送消息
	err = r.channel.Publish(
		r.Exchange,  //交换机名称，如果为空则使用默认交换机
		r.QueueName, //队列名
		false,       //是否持久化,如果为true,根据exchange和routingkey,会找到一个相应的队列，消息持久化到该队列中。如果无法找到，则直接将消息返回给生产者
		false,       //是否等待服务器确认，如果为true，则服务器会将消息持久化到磁盘后才确认发送成功
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message:%s", err)
	}
}

// PublishPubSub 订阅模式下生产者发送消息
func (r *RabbitMQ) PublishPub(message string) {
	//1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"fanout",   //交换机类型,广播模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"nge")
	//2.发送消息
	r.channel.Publish(
		r.Exchange, //交换机名称
		"",         //路由键，如果为空则使用默认的交换机
		false,      //是否持久化
		false,      //是否等待服务器确认
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message:%s", err)
	}

}

// PublishRouting 路由模式下生产者发送消息
func (r *RabbitMQ) PublishRouting(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"direct",   //交换机类型,direct模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"nge")
	//2.发送消息
	r.channel.Publish(
		r.Exchange, //交换机名称
		r.Key,      //路由键，如果为空则使用默认的交换机
		false,      //是否持久化
		false,      //是否等待服务器确认
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message:%s", err)
	}
	log.Printf(" [x] Sent %s", message)
}

// PublishTopic topic模式下生产者发送消息
func (r *RabbitMQ) PublishTopic(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"topic",    //交换机类型,direct模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"nge")
	//2.发送消息
	r.channel.Publish(
		r.Exchange, //交换机名称
		r.Key,      //路由键，如果为空则使用默认的交换机
		false,      //是否持久化
		false,      //是否等待服务器确认
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message:%s", err)
	}
}

// ConsumeSimple 消费者
func (r *RabbitMQ) ConsumeSimple() {
	//申请队列，如果队列不存在则创建，如果存在跳过创建
	//保证在队列存在时，消息能发送到队列
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false, // 队列是否持久化
		false, // 是否自动删除
		false, // 是否排他
		false, // 是否等待
		nil,   // 额外参数
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue:%s", err)
	}
	msg, err := r.channel.Consume(
		r.QueueName, //队列名
		"",          //消费者名称,用来区分多个消费者
		true,        //是否自动确认
		false,       //是否排他
		false,       //是否持久化，如果为true,则表示阻塞获取消息，如果为false,则表示非阻塞获取消息
		false,       //是否阻塞
		nil,         //额外参数
	)
	if err != nil {
		log.Fatalf("Failed to consume a message:%s", err)
	}
	forever := make(chan bool)
	//启用协程处理消息
	go func() {
		for d := range msg {
			log.Printf("Received a message:%s", d.Body)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

// ConsumePub 订阅阅模式下消费端代码
func (r *RabbitMQ) ConsumePub() {
	// 1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"fanout",   //交换机类型,广播模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"ange")
	// 2.尝试创建队列，这里注意队列名称不要写,因为队列名称是随机生成的
	q, err := r.channel.QueueDeclare(
		"",    //队列名称,随机生成
		false, //是否持久化
		false, //是否自动删除
		true,  //是否排他
		false, //是否等待
		nil,   //额外参数
	)
	r.failOnErr(err, "Failed to declare a queue")
	// 3.绑定队列和交换机
	r.channel.QueueBind(
		q.Name,     //队列名称
		"",         //在pub/sub模式下，路由键为空
		r.Exchange, //交换机名称
		false,      //是否等待
		nil,        //额外参数
	)
	// 4.消费消息
	message, err := r.channel.Consume(
		q.Name, //队列名称
		"",     //消费者名称，用来区分多个消费者
		true,   //是否自动确认
		false,  //是否排他
		false,  //是否持久化，如果为true,则表示阻塞获取消息，如果为false,则表示非阻塞获取消息
		false,  //是否阻塞
		nil,    //额外参数
	)
	forever := make(chan bool)
	go func() {
		for d := range message {
			log.Printf("Received a message:%s", d.Body)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

// ConsumeRouting 路由模式下消费端代码
func (r *RabbitMQ) ConsumeRouting() {
	// 1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"direct",   //交换机类型,路由模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"ange")
	// 2.尝试创建队列，这里注意队列名称不要写,因为队列名称是随机生成的
	q, err := r.channel.QueueDeclare(
		r.QueueName, //队列名称
		false,       //是否持久化
		false,       //是否自动删除
		true,        //是否排他
		false,       //是否等待
		nil,         //额外参数
	)
	r.failOnErr(err, "Failed to declare a queue")
	// 3.绑定队列和交换机
	r.channel.QueueBind(
		q.Name, //队列名称
		r.Key,
		r.Exchange,
		false,
		nil,
	)
	// 4.消费消息
	message, err := r.channel.Consume(
		q.Name, //队列名称
		"",     //消费者名称，用来区分多个消费者
		true,   //是否自动确认
		false,  //是否排他
		false,  //是否持久化，如果为true,则表示阻塞获取消息，如果为false,则表示非阻塞获取消息
		false,  //是否阻塞
		nil,    //额外参数
	)
	forever := make(chan bool)
	go func() {
		for d := range message {
			log.Printf("Received a message:%s", d.Body)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

// ConsumeTopic 主题模式下消费端代码
// 要注意key的规则
// 其中*表示匹配一个单词，#用于匹配多个单词（可以是零个）
// 匹配imooc.*表示匹配imooc开头的单词，
// *imooc表示结尾是imooc的单词，imooc.*表示包含imooc的单词
// 匹配#表示匹配任意多个单词，imooc.#表示匹配imooc开头的任意多个单词
func (r *RabbitMQ) ConsumeTopic() {
	//1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange, //交换机名称
		"topic",    //交换机类型,主题模式
		true,       //是否持久化
		false,      //是否自动删除
		false,      //是否排他,true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange的绑定
		false,      //是否等待
		nil,        //额外参数
	)
	r.failOnErr(err, "Failed to declare an exchange"+"ange")
	// 2.尝试创建队列，这里注意队列名称不要写,因为队列名称是随机生成的
	q, err := r.channel.QueueDeclare(
		r.QueueName, //队列名称
		false,       //是否持久化
		false,       //是否自动删除
		true,        //是否排他
		false,       //是否等待
		nil,         //额外参数
	)
	r.failOnErr(err, "Failed to declare a queue")
	// 3.绑定队列和交换机
	r.channel.QueueBind(
		q.Name, //队列名称
		r.Key,
		r.Exchange,
		false,
		nil,
	)
	// 4.消费消息
	messges, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	forever := make(chan bool)
	go func() {
		for d := range messges {
			log.Printf("Received a message:%s", d.Body)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
