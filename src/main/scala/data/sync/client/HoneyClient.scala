package data.sync.client

import akka.remote.{AssociationErrorEvent, DisassociatedEvent, RemotingLifecycleEvent}
import akka.actor._
import com.typesafe.config.ConfigFactory
import data.sync.common.ClientMessages._
import data.sync.common._
import net.sf.json.JSONObject
import org.apache.commons.lang.StringUtils
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.io.Source
import scala.concurrent.duration._

/**
 * Created by hesiyuan on 15/6/19.
 */
class HoneyClient {

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
  val urlPattern = "akka.tcp://%s@%s:%s/user/%s"
  val hostAndPorts = conf.getStrings(Constants.QUEENS_HOSTPORT)
  val masterAkkaUrls = hostAndPorts.map(e => {
    val hp = e.split(":")
    urlPattern.format(Constants.QUEEN_NAME, hp(0), hp(1), Constants.QUEEN_NAME)
  })

  def stopClient(): Unit = {
    system.shutdown()
  }

  lazy val system = ActorSystem("Client", akkaConf)
  val actor = system.actorOf(Props(new HoneyClientActor))
  var master: ActorSelection = null
  var masterAddress: Address = null
  while (master == null) {
    Thread.sleep(1000)
    print("Waiting for queen to connect...")
  }
  val actorRef: ActorRef = Await.result(master.resolveOne()(Duration.create(600, "seconds")), Duration.create(600, "seconds"))

  class HoneyClientActor extends Actor with ActorLogReceive with Logging {
    override def receiveWithLogging = {
      case RegisteredClient(masterUrl) =>
        if (!registered) {
          registered = true
          changeMaster(masterUrl)
        }
      case DisassociatedEvent(_, address, _) if address == masterAddress =>
        println(s"Connection to $address failed; waiting for queen to reconnect...")
        registerWithMaster
      case AssociationErrorEvent(cause, _, address, _, _) if isPossibleMaster(address) =>
        println(s"Could not connect to $address: $cause")
    }

    def tryRegisterAllMasters() {
      for (masterAkkaUrl <- masterAkkaUrls) {
        println("Connecting to queen " + masterAkkaUrl + "...")
        val actor = context.actorSelection(masterAkkaUrl)
        actor ! RegisterClient
      }
    }

    def registerWithMaster() {
      tryRegisterAllMasters()
      import context.dispatcher
      var retries = 0
      registrationRetryTimer
      match {
        case Some(_) =>
          println("Not spawning another attempt to register with the master, since there is an" +
            " attempt scheduled already.")
        case None => registrationRetryTimer = Some {
          context.system.scheduler.schedule(REGISTRATION_TIMEOUT, 5.seconds) {
            retries += 1
            if (registered) {
              registrationRetryTimer.foreach(_.cancel())
            } else if (retries >= REGISTRATION_RETRIES) {
              println("All queens are unresponsive! Giving up.")
              System.exit(1)
            } else {
              tryRegisterAllMasters()
            }
          }
        }
      }
    }

    def changeMaster(url: String) {
      activeMasterUrl = url
      masterAddress = AkkaUtils.extractAddressFromUrl(url)
      master = context.actorSelection(url)
      registrationRetryTimer.foreach(_.cancel())
      registrationRetryTimer = None
    }

    private def isPossibleMaster(remoteUrl: Address) = {
      masterAkkaUrls.map(AddressFromURIString(_).hostPort).contains(remoteUrl.hostPort)
    }

    override def postStop() {
      registrationRetryTimer.foreach(_.cancel())
    }

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      try {
        registerWithMaster()
      } catch {
        case e: Exception =>
          println("Failed to connect to master", e)
          context.stop(self)
      }
    }

    val REGISTRATION_TIMEOUT = 5.seconds
    val REGISTRATION_RETRIES = 300
    var registered = false
    var activeMasterUrl: String = null
    var registrationRetryTimer: Option[Cancellable] = None
  }


  def submitJobToQueen(job: SubmitJob): String = {
    AkkaUtils.askWithReply[SubmitResult](job, actorRef, Duration.create(21474835, "seconds")).toString
  }

  def killJob(kill: KillJob): String = {
    AkkaUtils.askWithReply[KillJobResult](kill, actorRef, Duration.create(21474835, "seconds")).toString
  }
}


object HoneyClient {
  var outputPath: String = null
  var taskNum: Int = 0
  var isSubmit: Boolean = false
  var isKill: Boolean = false
  var jobId: String = null
  var jobName: String = null
  var confPath: String = null
  var params:String =null
  var user = System.getProperties.getProperty("user.name") + "@" + NetUtil.getHostname

  def main(args: Array[String]) {
    var client: HoneyClient = null
    if (args.length == 0)
      printUsageAndExit(1)
    else {
      parseOpts(args.toList)
      if (isSubmit) {
        if (StringUtils.isEmpty(confPath)) {
          println("miss job-conf-path [-conf]")
          System.exit(1)
        }
        val source = Source.fromFile(confPath, "UTF-8")
        var desc = source.mkString
        if(params!=null){
          val kvs = params.split(",")
          for(kv <- kvs){
            val kva = kv.split("=")
            val k = "$"+kva(0)
            val v = kva(1)
            desc = desc.replace(k.trim,v.trim)
          }
        }
        println(desc)
        val submitJob = parseJSONObjectToSubmitJob(JSONObject.fromObject(desc))
        if (taskNum > 0) {
          submitJob.taskNum = taskNum
        } else if (submitJob.taskNum == 0) {
          submitJob.taskNum = 2
        }
        if (StringUtils.isNotEmpty(outputPath))
          submitJob.targetDir = outputPath
        if (StringUtils.isNotEmpty(jobName))
          submitJob.jobName = jobName
        if (StringUtils.isEmpty(submitJob.targetDir)) {
          println("miss job-output-path [-output]")
          System.exit(1)
        }
        if (StringUtils.isEmpty(submitJob.jobName)) {
          println("miss job-name [-name]")
          System.exit(1)
        }
        client = new HoneyClient()
        println(client.submitJobToQueen(submitJob))

      }
      if (isKill) {
        if (StringUtils.isEmpty(jobId)) {
          printUsageAndExit(1)
        }
        client = new HoneyClient()
        println(client.killJob(KillJob(jobId)))
      }
      client.stopClient()
    }

  }

  private def printConflictInfo(): Unit = {
    println("Action cannot be both Submit and Kill.")
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      println("Unknown/unsupported param " + unknownParam)
    }
    println(
      """
        |Usage: honey -submit [options]
        |Usage: honey -kill [job ID]
        |
        |Options:
        |  --output  job-output-path
        |  --number  job-task-number
        |  --conf    job-conf-path
        |  --name    job-name
        |  --param   job-param [format:paramName=paramValue,paramName=paramValue]
        |  --help, -h

      """.stripMargin
    )
    System.exit(exitCode)
  }

  def parseJSONObjectToSubmitJob(json: JSONObject): SubmitJob = {
    SubmitJob(json.getInt("priority"), json.getJSONArray("dbinfos").map(e => {
      val jobj = e.asInstanceOf[JSONObject]
      val tables = jobj.getJSONArray("tables").map(_.asInstanceOf[String]).toArray
      DBInfo(jobj.getString("sql"),
        jobj.getString("indexFiled"),
        tables, jobj.getString("db"),
        jobj.getString("ip"),
        jobj.getString("port"),
        jobj.getString("user"),
        jobj.getString("pwd"))
    }
    ).toArray, json.getInt("taskNum"), json.getString("callbackCMD"), json.getString("notifyUrl"), user, json.getString("jobName"), json.getString("targetDir"),if(json.containsKey("codec")) json.getString("codec") else "")
  }

  private def parseOpts(opts: Seq[String]): Unit = {
    parse(opts)
    def parse(opts: Seq[String]): Unit = opts match {
      case ("-submit") :: tail =>
        isSubmit = true
        if (isKill) {
          printConflictInfo
        }
        parse(tail)
      case ("--param") :: value :: tail =>
        params = value
        parse(tail)
      case ("--conf") :: value :: tail =>
        confPath = value
        isSubmit = true
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
        printUsageAndExit(1, value)

      case value :: tail =>
        printUsageAndExit(1)

      case Nil =>
    }
  }

}