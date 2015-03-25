package com.roundup.kafka.actors

import akka.actor._
import akka.testkit.TestProbe


class ProxyConsumerActor(collector: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    case MessageReady => sender ! RequestMessage
    case Message(payload) =>
      collector.forward(new String(payload).toInt)
      sender ! RequestMessage
  }
}

class KafkaOnActorsOrderTest extends KafkaOnActorsTestBase {
  val total = 100

  "Order test" should {
    s"publish and consume $total messages in order" in {
      val collector = TestProbe()
      val topics = Seq(TopicConfig(Topic, Props(new ProxyConsumerActor(collector.ref)), 1))

      val kafka = KafkaTestDriver(topics = topics)
      for { n <- 1 to total } kafka ! (Topic, s"$n".getBytes)

      collector.expectMsgAllOf( 1 to total : _* )
    }
  }
}

