package data.sync.client

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import data.sync.common.ClientMessages.SubmitJob
import data.sync.common.{Constants, Configuration}
import scala.collection.JavaConversions._
/**
 * Created by hesiyuan on 15/6/19.
 */
object HoneyClient {
  val conf = new Configuration()
  conf.addResource(Constants.CONFIGFILE_NAME)


  val akkaConf = ConfigFactory.parseMap(scala.collection.mutable.Map[String, String]())
    .withFallback(ConfigFactory.parseString(
    s"""
       |akka.daemonic = on
       |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
       |akka.stdout-loglevel = "ERROR"
       |akka.jvm-exit-on-fatal-error = off
       |akka.remote.require-cookie = "off"
       |akka.remote.secure-cookie = ""
       |akka.remote.transport-failure-detector.heartbeat-interval = 1000s
       |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = 6000s
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      """.stripMargin))


  val system = ActorSystem("Client",akkaConf)
  val urlPattern = "akka.tcp://%s@%s:%d/user/%s"
  val host = conf.get(Constants.QUEEN_ADDR,Constants.QUEEN_ADDR_DEFAULT)
  val port = conf.getInt(Constants.QUEEN_PORT,Constants.QUEEN_PORT_DEFAULT)
  val queenUrl = urlPattern.format(Constants.QUEEN_NAME,host,port,Constants.QUEEN_NAME)
  println(queenUrl)
  val greeter = system.actorSelection(queenUrl)
  println(greeter)
  def main (args: Array[String]) {
    greeter ! SubmitJob(1,Array(),2,"test")
    Thread.sleep(100000)
  }
}
