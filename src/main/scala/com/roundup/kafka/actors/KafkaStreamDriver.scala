package com.roundup.kafka.actors

import akka.actor.ActorRef
import com.roundup.kafka.actors.KafkaStreamDriver.NextMessage
import kafka.consumer.ConsumerTimeoutException

/**
 * Created by Lior Gonnen on 3/16/15.
 */
sealed trait KafkaStreamDriverMessage

case object MessageReady extends KafkaStreamDriverMessage

case object RequestMessage extends KafkaStreamDriverMessage

case class Message(payload: Array[Byte])

object KafkaStreamDriver {
    case object NextMessage extends KafkaStreamDriverMessage
}

private class KafkaStreamDriver(stream: KStream, config: TopicConfig) extends BaseKafkaActor {

    log.debug(s"Creating ${config.numWorkers} actors of type ${config.props}")
    val workers: Seq[ActorRef] = for (n <- 1 to config.numWorkers) yield context.actorOf(config.props)
    val streamIterator = stream.iterator()

    notifyWorkersMessageReady

    override def receive = {

        case RequestMessage =>
            try {
                if (streamIterator.hasNext())
                    sender ! Message(streamIterator.next.message)
                else
                    notifySelf
            }
            catch {
                case e: ConsumerTimeoutException => notifySelf // Timeout exceptions are ok and should occur every "consumer.timeout.ms" millis
            }

        case NextMessage =>
            try {
                if (streamIterator.hasNext())
                    notifyWorkersMessageReady
                else
                    notifySelf
            }
            catch {
                case e: ConsumerTimeoutException => notifySelf
            }
    }

    def notifyWorkersMessageReady: Unit = workers.foreach(_ ! MessageReady)

    /**
     * Send a message to self to check
     */
    def notifySelf: Unit = self ! NextMessage
}