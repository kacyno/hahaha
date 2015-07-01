package data.sync.client

import java.util.concurrent.TimeUnit
import akka.util.Timeout
import akka.util.Timeout._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import data.sync.common.ClientMessages.{SubmitResult, DBInfo, SubmitJob}
import data.sync.common.{AkkaUtils, Constants, Configuration}
import net.sf.json.JSONObject
import net.sf.json.util.JSONUtils
import org.apache.commons.beanutils.BeanUtils
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.io.Source

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


  val system = ActorSystem("Client", akkaConf)
  val urlPattern = "akka.tcp://%s@%s:%d/user/%s"
  val host = conf.get(Constants.QUEEN_ADDR, Constants.QUEEN_ADDR_DEFAULT)
  val port = conf.getInt(Constants.QUEEN_PORT, Constants.QUEEN_PORT_DEFAULT)
  val queenUrl = urlPattern.format(Constants.QUEEN_NAME, host, port, Constants.QUEEN_NAME)
  println(queenUrl)
  val greeter = system.actorSelection(queenUrl)

  def submitJob(job: SubmitJob): String = {
    val actorRef = Await.result(greeter.resolveOne()(Duration.create(5, "seconds")), Duration.create(5, "seconds"))
    AkkaUtils.askWithReply[SubmitResult](job, actorRef, Duration.create(5, "seconds")).toString
  }

  def main(args: Array[String]) {
    if (args.length != 1)
      println("Usage: HoneyClient [job-conf-path]")
    else {
      val source = Source.fromFile(args(0),"UTF-8")
      val desc = source.mkString
//      println(JSONObject.fromObject(parseJSONObjectToSubmitJob(JSONObject.fromObject(desc))))
      println(submitJob(parseJSONObjectToSubmitJob(JSONObject.fromObject(desc))))
    }
  }
  def parseJSONObjectToSubmitJob(json:JSONObject):SubmitJob={
    SubmitJob(json.getInt("priority"),json.getJSONArray("dbinfos").map(e=>{
      val jobj = e.asInstanceOf[JSONObject]
      val tables = jobj.getJSONArray("tables").map(_.asInstanceOf[String]).toArray
      DBInfo(jobj.getString("sql"),
        jobj.getString("indexFiled"),
        tables,jobj.getString("db"),
        jobj.getString("ip"),
        jobj.getString("port"),
        jobj.getString("user"),
        jobj.getString("pwd"))
    }
    ).toArray,json.getInt("taskNum"),json.getString("targetDir"))
  }
}
