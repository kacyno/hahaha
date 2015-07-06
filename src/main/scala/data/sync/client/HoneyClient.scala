package data.sync.client

import akka.util.Timeout._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import data.sync.common.ClientMessages._
import data.sync.common.{NetUtil, AkkaUtils, Constants, Configuration}
import net.sf.json.JSONObject
import org.apache.commons.lang.StringUtils
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
  var confPath:String = null
  var outputPath:String = null
  var taskNum:Int = 2
  var isSubmit:Boolean = false
  var isKill:Boolean = false
  var jobId:String = null
  var jobName:String = null
  var user = System.getProperties.getProperty("user.name")+"@"+NetUtil.getHostname
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
  val greeter = system.actorSelection(queenUrl)

  val actorRef = Await.result(greeter.resolveOne()(Duration.create(5, "seconds")), Duration.create(5, "seconds"))
  def submitJobToQueen(job: SubmitJob): String = {
    AkkaUtils.askWithReply[SubmitResult](job, actorRef, Duration.create(5, "seconds")).toString
  }
  def killJob(kill:KillJob):String={
    AkkaUtils.askWithReply[KillJobResult](kill, actorRef, Duration.create(50, "seconds")).toString
  }
  def main(args: Array[String]) {
    if (args.length ==0)
      printUsageAndExit(1)
    else {
      parseOpts(args.toList)
      if(isSubmit) {
        if(confPath==null)
          println("miss job-conf-path [-conf]")
        val source = Source.fromFile(confPath, "UTF-8")
        val desc = source.mkString
        //      println(JSONObject.fromObject(parseJSONObjectToSubmitJob(JSONObject.fromObject(desc))))
        val submitJob = parseJSONObjectToSubmitJob(JSONObject.fromObject(desc))
        if(submitJob.taskNum==0)
          submitJob.taskNum=taskNum
        if(StringUtils.isEmpty(submitJob.targetDir))
          submitJob.targetDir=outputPath
        if(StringUtils.isEmpty(submitJob.jobName))
          submitJob.jobName = jobName
        if(StringUtils.isEmpty(submitJob.targetDir))
          println("miss job-output-path [-output]")
        if(StringUtils.isEmpty(submitJob.targetDir))
          println("miss job-name [-name]")
        println(submitJobToQueen(submitJob))
      }
      if(isKill){
        println(killJob(KillJob(jobId)))
      }
    }
    system.shutdown()
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
    ).toArray,json.getInt("taskNum"),json.getString("callbackCMD"),json.getString("notifyUrl"),user,json.getString("jobName"),json.getString("targetDir"))
  }
  private def parseOpts(opts: Seq[String]): Unit = {
    parse(opts)
    def parse(opts: Seq[String]): Unit = opts match {
      case ("-submit")  :: tail =>
        isSubmit=true
        if(isKill){
          printConflictInfo
        }
        parse(tail)

      case ("--conf") :: value :: tail =>
        confPath = value
        isSubmit=true
        parse(tail)
      case ("--name") :: value :: tail =>
        jobName = value
        parse(tail)

      case ("--output") :: value :: tail =>
        outputPath = value
        parse(tail)

      case ("--number") :: value :: tail =>
         taskNum = value.toInt
        parse(tail)
      case ("-kill") :: value :: tail =>
        isKill = true
        jobId = value
        if (isSubmit) {
          printConflictInfo
        }
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(1)

      case value :: tail if value.startsWith("-") =>
        printUsageAndExit(1,value)

      case value :: tail =>
        printUsageAndExit(1)

      case Nil =>
    }
  }
  private def printConflictInfo(): Unit ={
    println("Action cannot be both Submit and Kill.")
  }
  private def printUsageAndExit(exitCode: Int,unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      println("Unknown/unsupported param " + unknownParam)
    }
    println(
      """Usage: honey -submit [options]
        |Usage: honey -kill [job ID]
        |
        |Options:
        |  --output  job-output-path
        |  --number  job-task-number
        |  --conf    job-conf-path
        |  --name    job-name
        |  --help, -h

      """.stripMargin
    )
    system.shutdown()
    System.exit(exitCode)
  }
}
