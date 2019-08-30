# Kafka Client

## Start the kafka Server

Start a container
```
$ docker-compose up -d
```
Send a message manually
```
$ docker-compose exec kafka sh
> /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic post
This is a message
This is another message
```

## Start the application

```
$ sbt run
```