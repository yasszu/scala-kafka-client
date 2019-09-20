# Kafka Client

## Start the kafka Server

Start a container
```
$ docker-compose up -d
```
Create topic
```
$ docker-compose exec broker kafka-topics --create --zookeeper \
  zookeeper:2181 --replication-factor 1 --partitions 1 --topic users
```

Show topics
```
$ docker-compose exec broker kafka-topics --list --bootstrap-server localhost:9092
```

## Start the application

```
$ sbt "runMain akka.Main app.Main"
```

## Redis
Check data
```
$ docker-compose exec redis redis-cli ZRANGE post:test 0 -1

```