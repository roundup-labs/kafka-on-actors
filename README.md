Akka Actors based Kafka consumer/producer for Scala
===

Supports Kafka 0.8.2

Concept
----
Kafka's consumer is inherently blocking, the idea is to have the consumer running in its own thread-pool
and delegate the messages to worker-actors that run in their own thread-pools while implementing the
[Work Pulling Pattern](http://doc.akka.io/docs/akka/snapshot/scala/howto.html) to prevent mailbox overflow
and without blocking.

```scala
val (zookepers, brokers, groupId) = ("zk1:2181,zk2:2181,zk3:2181", "kf1:9092,kf2:9092,kf3:9092", "test-group")
val topicConfigs = Seq(
    TopicConfig("topic1", Props(new TopicActor("topic1")), numWorkers = 10),
    TopicConfig("topic2", Props(new TopicActor("topic2")), numWorkers = 50)
)
val driver = KafkaDriver(actorSystem, zookepers, brockers, groupId, topicConfigs)

class TopicActor(topic: String) extends Actor {
    override def receive = {
        case MessageReady     => sender ! RequestMessage // request next message
        case Message(payload) =>
            //TODO: your code to handle payload
            sender ! RequestMessage // request next message
    }
}
```



<p align="center">
    <a href="https://play.google.com/store/apps/details?id=com.roundup&referrer=utm_source%3Dkafka-on-actors">
        <img src="http://static.roundupapp.co/RoundupLogoGooglePlayBadge.png">
    </a>
</p>
