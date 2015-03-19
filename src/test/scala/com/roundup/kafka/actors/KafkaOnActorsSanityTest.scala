package com.roundup.kafka.actors

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Created by Lior Gonnen on 3/17/15.
 */

class EchoConsumerActor extends Actor with ActorLogging {

    override def receive = {
        case MessageReady => sender ! RequestMessage
        case Message(payload) =>
            log.info(s"TestActor-Got message: ${new String(payload)}")
            sender ! RequestMessage
    }
}

class KafkaOnActorsSanityTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

    val ZooKeeper = "localhost:2181"
    val Brokers = "localhost:9092"
    val GroupId = "play"
    val topic = "test"


    def this() = this(ActorSystem("KafkaOnActors"))

    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    "This simple test" should {

        "print the messages to the log" in {
            val driver = KafkaDriver(system, ZooKeeper, Brokers, GroupId, Seq(TopicConfig(topic, Props[EchoConsumerActor], 1)))

            for (n <- 1 to 10) {
                driver !(topic, s"msg-$n")
            }

            expectNoMsg
        }
    }

}
