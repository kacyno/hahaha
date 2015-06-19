package data.sync.core

import akka.actor.{Props, Actor}
import akka.remote.DisassociatedEvent
import data.sync.common._
import data.sync.common.ClusterMessages._
import data.sync.common.Logging
import data.sync.http.server.HttpServer

/**
 * Created by hesiyuan on 15/6/19.
 */
class Queen extends Actor with ActorLogReceive with Logging{
  override def receiveWithLogging = {
    case RegisterBee(id,port,cores) =>
      logInfo("Registering Executor!!")
      println("queen")
      sender ! ClusterMessages.RegisteredBee
    case x: DisassociatedEvent =>
      logWarning(s"Received irrelevant DisassociatedEvent $x")
//    case unkown =>
//      logInfo(unkown.toString)
  }
}
object Queen extends Logging {

  private def run(hostname: String) {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("Queen", hostname, 8080)
    actorSystem.actorOf(
      Props(classOf[Queen]),
      name = "queen")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    val conf = new Configuration
    conf.addResource(Constants.CONFIGFILE_NAME)
    val httpServer = new HttpServer(conf)
    httpServer.start()
    run("localhost")
  }


}
