lazy val root = (project in file("."))
  .settings(
    name := "scala-kafka-client",
    version := "1.0",
    scalaVersion := "2.12.8",
    resolvers ++= Seq(
      "confluent" at "http://packages.confluent.io/maven/"
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.5.23",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.23" % Test,
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.23",
      "org.slf4j" % "slf4j-simple" % "1.7.26",
      "org.apache.kafka" %% "kafka" % "1.0.0",
      "org.apache.avro" % "avro" % "1.8.2",
      "io.confluent" % "kafka-avro-serializer" % "4.0.0",
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
      "net.codingwell" %% "scala-guice" % "4.2.6",
      "redis.clients" % "jedis" % "3.1.0"
    )
  )

(stringType in AvroConfig) := "String"