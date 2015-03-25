package com.roundup.kafka.actors

import akka.actor.{Props, ActorSystem}
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


trait KafkaOnActorsTestConfig {
  val ZooKeeper = "localhost:2181"
  val Brokers   = "localhost:9092"

  lazy val GroupId = randGroupId
  lazy val Topic   = randTopic

  def randId    = math.abs(util.Random.nextInt)
  def randGroupId = s"test-group-$randId"
  def randTopic   = s"test-$randId"
}


class KafkaOnActorsTestBase(_system: ActorSystem)
  extends TestKit(_system)
  with KafkaOnActorsTestConfig
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
{
  def this() = this(ActorSystem("kafka-on-actors"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  object KafkaTestDriver {
    def apply(groupId: String = GroupId, topics: Seq[TopicConfig]): TestActorRef[KafkaDriver] = {
      val props = Props(new KafkaDriver(ZooKeeper, Brokers, groupId, topics))
      TestActorRef[KafkaDriver](props)
    }
  }

  def withKafka[T](groupId: String = GroupId, topics: Seq[TopicConfig])
                  (f: TestActorRef[KafkaDriver] => T): T = {
    f(KafkaTestDriver(groupId, topics))
  }

}

