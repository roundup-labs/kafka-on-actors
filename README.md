Actor-based Kafka producer/consumer message driver for Scala
===

Supports Kafka 0.8.2

Concept
----
Kafka's consumer is inherently blocking, the idea is to have the consumer running in its own thread-pool
and delegate the messages to worker-actors that run in their own thread-pools while implementing the
[Work Pulling Pattern](http://doc.akka.io/docs/akka/snapshot/scala/howto.html) to prevent mailbox overflow
and without blocking.

```
val topicConfigs = Seq(
    TopicConfig("topic1", Props[TopicOneActor], 10),
    TopicConfig("topic2", Props[TopicTwoActor], 50),
    ...
)
val driver = KafkaDriver(system, ZooKeeper, Brokers, GroupId, topicConfigs)
```



<p align="center">
    <a href="https://play.google.com/store/apps/details?id=com.roundup&referrer=utm_source%3Dkafka-on-actors">
        <img src="http://static.roundupapp.co/RoundupLogoGooglePlayBadge.png">
    </a>
</p>
