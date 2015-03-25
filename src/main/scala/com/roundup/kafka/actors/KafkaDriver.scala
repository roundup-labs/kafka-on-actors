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

    def apply(actorSystem: ActorSystem, zookeeper: String, brokers: String, groupId: String, topicConfigs: Seq[TopicConfig], consumerConfig: ConsumerConfig = null, producerConfig: ProducerConfig = null): ActorRef = {
        actorSystem.actorOf(Props(new KafkaDriver(zookeeper, brokers: String, groupId, topicConfigs)))
    }
}

/**
 * Create the main Kafka Driver actor
 * @param zookeeper The ZooKeeper connection string in the form hostname:port
 * @param brokers A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
 * @param groupId A string that uniquely identifies the group of consumer processes to which this consumer belongs.
 * @param topicConfigs The list of topics to consume and type and number of actors to use for consumption
 * @param consumerConfig Optional additional configuration for the Kafka consumer
 * @param producerConfig Optional additional configuration for the Kafka producer
 */
class KafkaDriver(
    zookeeper: String,
    brokers: String,
    groupId: String,
    topicConfigs: Seq[TopicConfig],
    consumerConfig: ConsumerConfig = null,
    producerConfig: ProducerConfig = null) extends BaseKafkaActor {

    val effectiveConsumerConfig = if (consumerConfig != null) consumerConfig.copy(zookeeper = zookeeper, groupId = groupId) else ConsumerConfig(zookeeper, groupId, true)
    val effectiveProducerConfig = if (producerConfig != null) producerConfig.copy(brokers = brokers) else ProducerConfig(brokers)
    val kafkaConsumer = ScalaKafkaConsumer(effectiveConsumerConfig)
    val kafkaProducer = ScalaKafkaProducer(effectiveProducerConfig)

    val streams: Map[String, KStream] = kafkaConsumer.createMessageStreams(topicConfigs.map(_.topic))

    val topicDrivers = for (config <- topicConfigs) yield {
        val props = Props(classOf[KafkaStreamDriver], streams(config.topic), config).withDispatcher("kafka-blocking-dispatcher")
        context.actorOf(props, name = s"TopicDriver-${config.topic}")
    }

    override def receive = {
        case (topic: String, payload: Array[Byte]) =>
            log.debug(s"Received message for topic $topic")
            kafkaProducer.send(topic, payload)
    }

    override def postStop: Unit = {
        super.postStop

        kafkaConsumer.close
        kafkaProducer.close
    }
}