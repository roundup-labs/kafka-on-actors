package com.roundup.kafka.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import kafka.consumer.KafkaStream

/**
 * Created by lior on 3/16/15.
 */

object `package` {
    type KStream = KafkaStream[Array[Byte], Array[Byte]]
}

case class TopicConfig(topic: String, props: Props, numWorkers: Int)

object KafkaDriver {

    def apply(actorSystem: ActorSystem, zookeeper: String, brokers: String, groupId: String, topicConfigs: Seq[TopicConfig]): ActorRef = {
        actorSystem.actorOf(Props(new KafkaDriver(zookeeper, brokers: String, groupId, topicConfigs)))
    }

}

class KafkaDriver(zookeeper: String, brokers: String, groupId: String, topicConfigs: Seq[TopicConfig]) extends BaseKafkaActor {

    val kafkaConsumer = ScalaKafkaConsumer(zookeeper, groupId)
    val kafkaProducer = ScalaKafkaProducer(brokers)

    val streams: Map[String, KStream] = kafkaConsumer.createMessageStreams(topicConfigs.map(_.topic))

    val topicDrivers = for (config <- topicConfigs) yield {
        val props = Props(classOf[KafkaStreamDriver], streams(config.topic), config).withDispatcher("kafka-blocking-dispatcher")
        context.actorOf(props, name = s"kafka-driver-topic-${config.topic}")
    }

    override def receive = {
        case (topic: String, payload: String) => kafkaProducer.send(topic, payload.getBytes)

        case (topic: String, payload: Array[Byte]) => kafkaProducer.send(topic, payload)

        case _ => log.warning("Received unhandled message type")
    }

    override def postStop: Unit = {
        super.postStop

        kafkaConsumer.close
        kafkaProducer.close
    }
}