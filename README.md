# go-rabbitmq
rabbitmq的消费者和生产者

# 在项目中安装使用
```shell
go get -u github.com/fbaiyi/go-rabbitmq/v2
```

# 测试
## 1、安装依赖
```shell
go mod tidy
```

## 运行example
配置 amqp
消费者
```shell
go run examples/consumer.go
go run examples/delay_consumer.go
```
生产者
```shell
go run examples/producer.go
go run examples/delay_producer.go
```
查看效果

## 2、单元测试
```shell
go test
```
