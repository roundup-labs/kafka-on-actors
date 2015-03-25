package com.roundup.kafka.actors

import akka.actor.{Actor, ActorLogging, Props}

/**
 * Created by Lior Gonnen on 3/17/15.
 */

class EchoConsumerActor extends Actor with ActorLogging {
    override def receive = {
      case MessageReady => sender ! RequestMessage
      case Message(payload) =>
        log.info("EchoConsumerActor received a message: {}", new String(payload))
        sender ! RequestMessage
    }
}

class KafkaOnActorsSanityTest extends KafkaOnActorsTestBase {
  val total = 10
  val topics = Seq(TopicConfig(Topic, Props[EchoConsumerActor], numWorkers = 1))

  "This a simple test" should {
    s"print $total messages to the log" in withKafka(topics = topics) { kafka =>

      for { n <- 1 to total } kafka ! (Topic, s"msg-$n".getBytes)

    }
  }
}