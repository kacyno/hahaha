package data.sync.client

import java.util.concurrent.TimeUnit
import akka.util.Timeout
import akka.util.Timeout._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import data.sync.common.ClientMessages.{SubmitResult, DBInfo, SubmitJob}
import data.sync.common.{AkkaUtils, Constants, Configuration}
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

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
  def main (args: Array[String]) {
    val message = SubmitJob(1,Array(DBInfo("select * from %s ", "id", Array[String]("import_cps_confirm_1"), "test", "localhost", "3306", "root", "lkmlnfqp")),2,"/Users/hesiyuan/honey-data/")
    val actorRef = Await.result(greeter.resolveOne()(Duration.create(5,"seconds")), Duration.create(5,"seconds"))
    println(AkkaUtils.askWithReply[SubmitResult](message,actorRef,Duration.create(5,"seconds")))
  }
}
