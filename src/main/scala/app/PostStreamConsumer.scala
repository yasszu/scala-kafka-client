package app

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import akka.util.Timeout
import app.util.Logging
import com.typesafe.config.Config
import example.avro.messages.Post
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

object PostStreamConsumer {

  case class Run()

  case class Subscribe(record: ConsumerRecord[String, Post])

}

class PostStreamConsumer extends Actor with Logging {

  import PostStreamConsumer._

  val groupId = "PostService"
  val topic = "posts"

  implicit val system: ActorSystem = context.system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(30.seconds)

  lazy val config: Config = system.settings.config
  lazy val akkaKafkaConfig: Config = config.getConfig("akka.kafka.consumer")
  lazy val kafkaConsumerConfig: Config = config.getConfig("kafka.consumer")
  lazy val bootstrapServer: String = kafkaConsumerConfig.getString("bootstrap.servers")
  lazy val schemaRegistryUrl: String = kafkaConsumerConfig.getString("schema.registry.url")
  lazy val avroDeserializer: String = kafkaConsumerConfig.getString("avro.deserializer")
  lazy val committerSettings = CommitterSettings(system)

  val postWriter: ActorRef = context.actorOf(Props[PostWriter])

  val kafkaAvroSerDeConfig: Map[String, Any] = Map[String, Any](
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl,
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
  )

  val valueDeserializer: Deserializer[Post] = {
    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)
    kafkaAvroDeserializer.asInstanceOf[Deserializer[Post]]
  }

  val consumerSettings: ConsumerSettings[String, Post] =
    ConsumerSettings(akkaKafkaConfig, new StringDeserializer, valueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer: RunnableGraph[DrainingControl[Done]] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        subscribe(msg.record).map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)

  def subscribe(record: ConsumerRecord[String, Post]) = Future {
    self ! Subscribe(record)
  }

  def runConsumer: DrainingControl[Done] = {
    log.info("Run consumer")
    consumer.run()
  }

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Run() => context.become(running(runConsumer))
  }

  def running(control: DrainingControl[Done]): Receive = {
    case Subscribe(record) =>
      postWriter ! PostWriter.Write(record)
    case Terminated(_) =>
      log.info("Stopping consumer")
      control.drainAndShutdown()
      context.stop(self)
  }

}
