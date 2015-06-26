package data.sync.core

import java.util
import java.util.{Date, Collections}
import java.util.concurrent.{ThreadPoolExecutor, Executors}
import akka.actor.{ActorSelection, Props, Actor}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import data.sync.common._
import ClusterMessages._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by hesiyuan on 15/6/19.
 */
class Bee(conf:Configuration) extends Logging {
  var driver: ActorSelection = null
  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)
  private val executorPool = Executors.newCachedThreadPool(daemonThreadFactoryBuilder.build()).asInstanceOf[ThreadPoolExecutor]
  private val workerSet = Collections.synchronizedSet(new util.HashSet[Worker]) //将正在运行的worker放在这里以便进行统一管理
  var beeId:String=null
  val queenHost = conf.get(Constants.QUEEN_ADDR,Constants.QUEEN_ADDR_DEFAULT)
  val queenPort = conf.getInt(Constants.QUEEN_PORT,Constants.QUEEN_PORT_DEFAULT)
  val beeHost = conf.get(Constants.BEE_ADDR,Constants.BEE_ADDR_DEFAULT)
  val beePort = conf.getInt(Constants.BEE_PORT,Constants.BEE_PORT_DEFAULT)
  val queenUrl = "akka.tcp://%s@%s:%d/user/%s".format(Constants.QUEEN_NAME,queenHost,queenPort,Constants.QUEEN_NAME)
  val beeWorkerNum = conf.getInt(Constants.BEE_WORKER_NUM,Constants.BEE_WORKER_NUM_DEFAULT)
  class BeeActor extends Actor with ActorLogReceive with Logging {
    override def receiveWithLogging = {
      case RegisteredBee(id) => beeId = id;logInfo("Registered Executor!!");startDriverHeartbeater
      case StartTask(tad) => startTask(tad)
      case KillJob(jobId) => killJob(jobId)
      case StopAttempt(attemptId)=> stopAttempt(attemptId)
      case x: DisassociatedEvent =>
        logWarning(s"Received irrelevant DisassociatedEvent $x")
        System.exit(1)
    }

    override def preStart() {
      logInfo("Connecting to driver: " + queenUrl)
      driver = context.actorSelection(queenUrl)
      driver ! RegisterBee (beeWorkerNum)
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }
  }
  private def stopAttempt(attemptId:String): Unit ={
    for(worker<-workerSet){
      if(worker.getAttempt.attemptId==attemptId){
        worker.getFetcher.stop();
        worker.getSinker.stop();
      }
    }
  }
  private def killJob(jobId:String): Unit ={
    for(worker<-workerSet){
      if(worker.getJobId==jobId){
        worker.getFetcher.stop();
        worker.getSinker.stop();
      }
    }
  }
  /*
  开始任务
   */
  private def startTask(tad: TaskAttemptInfo) = synchronized {
    logInfo("Get Task:"+tad)
    val worker = new Worker(tad, this)
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
  def failTask(worker:Worker): Unit ={
    worker.getAttempt.status = TaskAttemptStatus.FAILED
    worker.getFetcher.stop()
    worker.getSinker.stop()
    finishTask(worker)
  }
  def successTask(worker:Worker): Unit ={
    worker.getAttempt.status = TaskAttemptStatus.FINSHED
    finishTask(worker)
  }
  /*
  向queen进行状态汇报
   */
  def updateStatus() = {
    var buffer = ArrayBuffer[BeeAttemptReport]()
    for(worker<-workerSet){
      buffer+= BeeAttemptReport(worker.getAttempt.attemptId,
        worker.getReadCount,
        worker.getWriteCount,
        new Date().getTime,
        worker.getError,
        worker.getAttempt.status)
    }
    driver ! StatusUpdate(beeId,buffer.toArray)
  }

  /*
  周期性的向queen汇报自己的情况
   */
  def startDriverHeartbeater() {
    val interval = Constants.BEE_HEATBEATER_INTERVAL
    val t = new Thread() {
      override def run() {
        while (true) {
          try {
            updateStatus()
            Thread.sleep(interval)
          }catch{
            case e : Throwable=> logInfo("heartbeat erro",e)
          }
          }
      }
    }
    t.setDaemon(true)
    t.setName("Queue Heartbeater")
    t.start()

  }
  def run(): Unit ={
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(Constants.BEE_NAME, beeHost, beePort)
    actorSystem.actorOf(
      Props(new BeeActor),
      name = Constants.BEE_NAME)
    actorSystem.awaitTermination()
  }
}

object Bee extends Logging {
  def main(args: Array[String]) {
    val conf = new Configuration()
    conf.addResource(Constants.CONFIGFILE_NAME)
    new Bee(conf).run()
  }


}