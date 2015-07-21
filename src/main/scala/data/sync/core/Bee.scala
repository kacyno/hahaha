package data.sync.core

import java.util
import java.util.{TimerTask, Timer, Date, Collections}
import java.util.concurrent.{ThreadPoolExecutor, Executors}
import akka.actor._
import akka.remote.{RemoteActorRef, DisassociatedEvent, RemotingLifecycleEvent}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import data.sync.common._
import ClusterMessages._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by hesiyuan on 15/6/19.
 */
class Bee(conf: Configuration) extends Logging {
  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)

  //fetcher与sinker的运行线程池
  private val executorPool = Executors.newCachedThreadPool(daemonThreadFactoryBuilder.build()).asInstanceOf[ThreadPoolExecutor]

  //将正在运行的worker放在这里以便进行统一管理
  private val workerSet = Collections.synchronizedSet(new util.HashSet[Worker])

  //beeId，由queen指定
  var beeId: String = null


  val beeHost = conf.get(Constants.BEE_ADDR, Constants.BEE_ADDR_DEFAULT)
  val beePort = conf.getInt(Constants.BEE_PORT, Constants.BEE_PORT_DEFAULT)

  val hostAndPorts = conf.getStrings(Constants.QUEENS_HOSTPORT)
  val queenUrls = hostAndPorts.map(e => {
    val hp = e.split(":")
    "akka.tcp://%s@%s:%s/user/%s".format(Constants.QUEEN_NAME, hp(0), hp(1), Constants.QUEEN_NAME)
  })

  val beeWorkerNum = conf.getInt(Constants.BEE_WORKER_NUM, Constants.BEE_WORKER_NUM_DEFAULT)

  var master: ActorSelection = null

  var activeMasterUrl: String = ""

  var masterAddress: Address = null

  var registrationRetryTimer: Option[Timer] = None

  @volatile var registered = false
  @volatile var connected = false

  var connectionAttemptCount = 0



  class BeeActor extends Actor with ActorLogReceive with Logging {

    override def receiveWithLogging = {
      case RegisteredBee(queenUrl, id) =>
        beeId = id;
        logInfo("Registered Executor!!")

        changeMaster(queenUrl)
      case StartTask(tad) => startTask(tad)
      case KillJob(jobId) =>
        logInfo("Get message KillJob:"+jobId)
        killJob(jobId)
      case StopAttempt(attemptId) => stopAttempt(attemptId)
//      case ReregisterWithMaster =>
//        reregisterWithMaster()
      case x: DisassociatedEvent if x.remoteAddress == masterAddress =>
        stopAll()
        masterDisconnected()
      case e =>println(e.toString)
    }

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      //连接Queen
      registerWithMaster()
    }

    private def tryRegisterAllMasters() {
      for (masterAkkaUrl <- queenUrls) {
        logInfo("Connecting to queen " + masterAkkaUrl + "...")
        val actor =context.actorSelection(masterAkkaUrl)
        actor ! RegisterBee(beeWorkerNum)
      }
    }

    def changeMaster(url: String) {
      logInfo("change master "+url)
      registered = true
      activeMasterUrl = url
      master = context.actorSelection(url)
      masterAddress = AkkaUtils.extractAddressFromUrl(url)
      connected = true
      logInfo("cancel registrationRetryTimer by changeMaster")
      registrationRetryTimer.foreach(_.cancel())
      registrationRetryTimer = None
      startDriverHeartbeater
    }

    private def masterDisconnected() {
      logError("Connection to queen failed! Waiting for master to reconnect...")
      connected = false
      stopDriverHeaertbeater

      master = null
      masterAddress=null

      registerWithMaster()
    }


    private def reregisterWithMaster(): Unit = {
      connectionAttemptCount += 1
      logInfo("reregisterWithMaster "+connectionAttemptCount)
      if (registered) {
        logInfo("cancel registrationRetryTimer by reregisterWithMaster")
        registrationRetryTimer.foreach(_.cancel())
        registrationRetryTimer = None
      } else if (connectionAttemptCount <= 100) {
        logInfo(s"Retrying connection to queen (attempt # $connectionAttemptCount)")
        if (master != null) {
          master ! RegisterBee(beeWorkerNum)
        } else {
          tryRegisterAllMasters()
        }
        if (connectionAttemptCount == 60) {
          registrationRetryTimer.foreach(_.cancel())
          registrationRetryTimer = Some {
            val timer = new Timer()
              timer.schedule(new TimerTask{
              override def run(): Unit = {
                reregisterWithMaster()
              }
            },60*1000,60*1000)
            timer
          }
        }
      } else {
        logError("All queens are unresponsive! Giving up.")
        System.exit(1)
      }
    }

    def registerWithMaster() {
      registrationRetryTimer match {
        case None =>
          logInfo("create registrationRetryTimer")
          registered = false
          tryRegisterAllMasters()
          connectionAttemptCount = 0
          registrationRetryTimer = Some {
            val timer = new Timer()
            timer.schedule(new TimerTask {
              override def run(): Unit = {
                reregisterWithMaster()
              }
            },5*1000,5*1000)

            timer
          }
        case Some(_) =>
          logInfo("Not spawning another attempt to register with the queen, since there is an" +
            " attempt scheduled already.")
      }
    }
  }

  private def stopAttempt(attemptId: String): Unit = {
    for (worker <- workerSet) {
      if (worker.getAttempt.attemptId == attemptId) {
        logInfo("Stop Attempt:"+attemptId)
        worker.getFetcher.stop();
        worker.getSinker.stop();
      }
    }
  }

  private def stopAll(): Unit = {
    logInfo("Stop All Worker...")
    for (worker <- workerSet) {
      worker.getFetcher.stop()
      worker.getSinker.stop()
    }
  }

  private def killJob(jobId: String): Unit = {
    for (worker <- workerSet) {
      if (worker.getJobId == jobId) {
        worker.getFetcher.stop();
        worker.getSinker.stop();
      }
    }
  }

  /*
  开始任务
   */
  private def startTask(tad: TaskAttemptInfo) = synchronized {
    logInfo("Get Task:" + tad)
    val worker = new Worker(conf, tad, this)
    worker.getAttempt.status = TaskAttemptStatus.RUNNING
    executorPool.execute(worker.getFetcher)
    executorPool.execute(worker.getSinker)
    workerSet += worker
  }

  /*
  结束任务，包括正常与非正常
   */
  def finishTask(worker: Worker) = synchronized {
    updateStatus();
    workerSet -= worker
  }

  def failTask(worker: Worker): Unit = {
    worker.getAttempt.status = TaskAttemptStatus.FAILED
    worker.getFetcher.stop()
    worker.getSinker.stop()
    finishTask(worker)
  }

  def successTask(worker: Worker): Unit = {
    worker.getAttempt.status = TaskAttemptStatus.FINISHED
    finishTask(worker)
  }

  /*
  向queen进行状态汇报
   */
  def updateStatus() = {
    if (master != null) {
      var buffer = ArrayBuffer[BeeAttemptReport]()
      for (worker <- workerSet) {
        buffer += BeeAttemptReport(beeId, worker.getAttempt.attemptId,
          worker.getReadCount,
          worker.getWriteCount,
          worker.getBufferSize,
          new Date().getTime,
          worker.getError,
          worker.getAttempt.status)
      }
      master ! StatusUpdate(beeId, buffer.toArray)
    }
  }

  @volatile var heartbeat = false
  var heartBeatThread: Thread = null

  def stopDriverHeaertbeater(): Unit = {
    heartbeat = false
    if(heartBeatThread!=null)
    heartBeatThread.interrupt
  }

  /*
  周期性的向queen汇报自己的情况
   */
  def startDriverHeartbeater() {
    heartbeat = true
    val interval = Constants.BEE_HEARTBEATER_INTERVAL
    heartBeatThread = new Thread() {
      override def run() {
        while (heartbeat) {
          try {
            Thread.sleep(interval)
            updateStatus()
          } catch {
            case e: Throwable => logInfo("heartbeat error", e)
          }
        }
      }
    }
    heartBeatThread.setDaemon(true)
    heartBeatThread.setName("Queue Heartbeater")
    heartBeatThread.start()

  }

  def run(): Unit = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(Constants.BEE_NAME, beeHost, beePort)
    actorSystem.actorOf(
      Props(new BeeActor),
      name = Constants.BEE_NAME)
    Bee.printBee()
    actorSystem.awaitTermination()

  }
}

object Bee extends Logging {
  def main(args: Array[String]) {
    val conf = new Configuration()
    conf.addResource(Constants.CONFIGFILE_NAME)
    new Bee(conf).run()
  }

  def printBee(): Unit = {
    logInfo(
      """
        |
        |                 \     /
        |             \    o ^ o    /
        |               \ (     ) /
        |    ____________(%%%%%%%)____________
        |   (     /   /  )%%%%%%%(  \   \     )
        |   (___/___/__/           \__\___\___)
        |      (     /  /(%%%%%%%)\  \     )
        |       (__/___/ (%%%%%%%) \___\__)
        |               /(       )\
        |             /   (%%%%%)   \
        |                  (%%%)
        |                    !
      """.stripMargin)
  }

}