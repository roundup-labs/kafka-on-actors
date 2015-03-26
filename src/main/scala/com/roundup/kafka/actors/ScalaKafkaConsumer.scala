package com.roundup.kafka.actors

import java.util.Properties

import kafka.consumer.Consumer
import kafka.utils.Logging

import scala.collection.JavaConverters._

/**
 * Created by Lior Gonnen on 3/16/15.
 */

/**
 * Wraps a Kafka Java consumer.
 * @param zookeeper
 *        Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a
 *        ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is down you can also
 *        specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.
 *
 * @param groupId
 *        A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the
 *        same group id multiple processes indicate that they are all part of the same consumer group.
 *
 * @param autoOffsetResetToStart
 *        What to do when there is no initial offset in ZooKeeper or if an offset is out of range:
 *        true translates to "smallest": automatically reset the offset to the smallest offset
 *        false translates to "largest":  automatically reset the offset to the largest offset
 *
 * @param consumerTimeoutMillis
 *        Throw a timeout exception to the consumer if no message is available for consumption after the specified interval
 *
 * @param autoCommitIntervalMillis
 *        The frequency in ms that the consumer offsets are committed to zookeeper.
 */
case class ConsumerConfig(
    zookeeper: String = null,
    groupId: String = null,
    autoOffsetResetToStart: Boolean = false,
    consumerTimeoutMillis: Long = 500,
    autoCommitIntervalMillis: Long = 10000) {

    def validate = {
        require(zookeeper != null, "null zookeeper")
        require(groupId != null, "null groupId")
    }

    def toProperties = new Properties() {
        put("zookeeper.connect", zookeeper)
        put("group.id", groupId)
        put("zookeeper.session.timeout.ms", "5000")                          // ZooKeeper session timeout. If the consumer fails to heartbeat to ZooKeeper for this period of time it is considered dead and a rebalance will occur.
        put("zookeeper.sync.time.ms", "200")                                 // How far a ZK follower can be behind a ZK leader
        put("auto.commit.interval.ms", autoCommitIntervalMillis.toString)    // The frequency in ms that the consumer offsets are committed to zookeeper.
        put("consumer.timeout.ms", consumerTimeoutMillis.toString)           // Throw a timeout exception to the consumer if no message is available for consumption after the specified interval (default = -1 - blocking)
        put("auto.offset.reset", if (autoOffsetResetToStart) "smallest" else "largest")
    }
}

case class ScalaKafkaConsumer(config: ConsumerConfig) extends Logging {

    require(config != null)

    config.validate

    val consumer = Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(config.toProperties))

    def createMessageStreams(topics: Seq[String]): Map[String, KStream] = {
        type JMap[K,V] = java.util.Map[K, V]
        type JInt = java.lang.Integer
        val javaMap = consumer.createMessageStreams(topics.map((_, 1)).toMap.asJava.asInstanceOf[JMap[String, JInt]])

        javaMap.asScala.map { case (topic, streamList) => (topic, streamList.get(0)) }.toMap // The last toMap is needed tp convert the mutable map to immutable
    }

    def close: Unit = {
        try {
            consumer.commitOffsets
            consumer.shutdown
        }
        catch {
            case e: Exception => error(e)
        }
    }
}
