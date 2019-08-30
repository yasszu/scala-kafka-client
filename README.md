# Akka Demo
## Start application

```
$ sbt run
```

## Start kafka

Start container
```
$ docker-compose up -d
```
Send message
```
$ docker-compose exec kafka sh
> /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic post
This is a message
This is another message
```