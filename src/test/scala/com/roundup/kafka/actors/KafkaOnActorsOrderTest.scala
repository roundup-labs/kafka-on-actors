package com.roundup.kafka.actors

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}



class ProxyConsumerActor(collector: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    case MessageReady => sender ! RequestMessage
    case Message(payload) =>
      collector ! new String(payload).toInt
      sender ! RequestMessage
  }
}

class KafkaOnActorsOrderTest extends KafkaOnActorsTestBase
{
  val total = 100

  "Order test" should {
    s"publish and consume $total messages in order" in {
      val collector = TestProbe()
      val topics = Seq(TopicConfig(Topic, Props(new ProxyConsumerActor(collector.ref)), 1))

      val kafka = KafkaTestDriver(topics = topics)
      for { n <- 1 to total } kafka ! (Topic, s"$n")

      collector.expectMsgAllOf( 1 to total : _* )
    }
  }
}

