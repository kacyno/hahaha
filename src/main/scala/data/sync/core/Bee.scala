package data.sync.core

import akka.actor.{ActorSelection, Props, Actor}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import data.sync
import data.sync.common._
import ClusterMessages._


/**
 * Created by hesiyuan on 15/6/19.
 */
class Bee(queueUrl: String) extends Actor with ActorLogReceive with Logging {
  var driver: ActorSelection = null

  override def receiveWithLogging = {
    case RegisteredBee=> logInfo("Registered Executor!!")
    case x: DisassociatedEvent =>
        logWarning(s"Received irrelevant DisassociatedEvent $x")
  }
  override def preStart() {
    logInfo("Connecting to driver: " + queueUrl)
    driver = context.actorSelection(queueUrl)
    driver ! RegisterBee("haha", "8088", 10)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }
}
object Bee extends Logging {

  private def run(queenUrl: String, hostname: String) {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("Bee", hostname, 8088)
    val hostPort = hostname + ":" + boundPort
    actorSystem.actorOf(
      Props(classOf[Bee],
        queenUrl),
      name = "Bee")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    var queenUrl: String = null
    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--queue-url") :: value :: tail =>
          queenUrl = value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          System.exit(1)
      }
    }
    run("akka.tcp://Queen@localhost:8080/user/queen", "localhost")
  }


}