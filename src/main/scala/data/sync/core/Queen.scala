package data.sync.core

import java.util.Date
import akka.actor.{ActorRef, Props, Actor}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import data.sync.common.ClientMessages.{SubmitResult, DBInfo, SubmitJob}
import data.sync.common._
import data.sync.common.ClusterMessages._
import data.sync.common.Logging
import data.sync.http.server.HttpServer

/**
 * Created by hesiyuan on 15/6/19.
 */
class Queen extends Actor with ActorLogReceive with Logging {
  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receiveWithLogging = {
    case RegisterBee(cores) =>
      val beeId = registerBee(cores, sender)
      sender ! ClusterMessages.RegisteredBee(beeId)
      assignTask()
    case SubmitJob(priority, dbinfos, taskNum, targetDir) =>
      logInfo("submit job from " + sender.path.address)
      val jobId = submitJob(priority, dbinfos, taskNum, targetDir)
      sender ! SubmitResult(jobId)
    case StatusUpdate(beeId, reports) => updateBees(beeId, reports)
    case x: DisassociatedEvent =>
      removeBee(x)
      logWarning(s"Received irrelevant DisassociatedEvent $x")
    case other=>
      logInfo(other.toString)
  }

  /*
   *Bee移除
   *  在BeeManager中移除该Bee
   *  将运行在该Bee上的taskAttempt全部移走
   */
  def removeBee(x: DisassociatedEvent): Unit = {
    val bee = BeeManager.getBeeByAddress(x.remoteAddress)
    if (bee != null) {
      BeeManager.removeBee(bee.beeId)
      JobManager.removeAttemptByBee(bee.beeId, this)
      assignTask()
    }
  }

  /*
   *Bee注册
   *1,  记录下bee的信息
   *2,  进行一次任务调度
   */
  def registerBee(cores: Int, sender: ActorRef): String = {
    val beeId = sender.path.address.hostPort
    logInfo("Registering Executor：" + beeId)
    BeeManager.updateBee(BeeDesc(0, cores, beeId, sender))
    beeId
  }

  /*
   * Bee汇报
   * 1, 完成的task,如果成功了则改task与taskAttempt状态为完成，
   *    如果失败了看是否需要再次attempt,如果不需要则将task及对应的job置为失败，同时结束所有该job的task
   *
   * 2, 正在远行的task,更新进度信息
   *
   * 3, 当worker有释放时，进行一次作业调度
   */
  def updateBees(beeId: String, reports: Array[BeeAttemptReport]): Unit = {
    var isWorkerChange = false
    for (report <- reports) {
      JobManager.processReport(beeId, report, this)
      if (report.status != TaskAttemptStatus.RUNNING) {
        isWorkerChange = true;
      }
    }
    if (isWorkerChange)
      assignTask();
  }

  /*
   *将作业拆分成多个TaskDesc,并封装在Job中放入队列
   */
  def submitJob(priority: Int, dbinfos: Array[DBInfo], num: Int, dir: String): String = {
    val jobId = IDGenerator.generatorJobId();
    val tasks = SimpleSplitter.split(jobId, dbinfos, num, dir)
    val job = JobInfo(jobId,
      priority,
      new Date().getTime,
      dir,
      dbinfos,
      tasks,
      new java.util.HashSet[TaskInfo](),
      new java.util.HashSet[TaskInfo](),
      JobStatus.SUBMITED
    )
    JobManager.addJob(job)
    //加入调度
    FIFOScheduler.addJob(job)
    assignTask()
    jobId
  }

  /*
  将任务分配给bee执行
  两个场景触发该方法，1）任务需要调度时 2）可用的的worker数发生变化时
   */
  def assignTask(): Unit = {
    val assigns = FIFOScheduler.assigns
    for ((beeId, tad) <- assigns) {
      logInfo("send task: "+tad +" to bee:"+beeId)
      BeeManager.getBee(beeId).sender ! StartTask(tad)
    }
  }

  /*
   *
   */
  def startChecker() {
    val interval = Constants.QUEEN_CHECKER_INTERVAL
    val t = new Thread() {
      override def run() {
        while (true) {
          Thread.sleep(interval)
          try {
            if (JobManager.checkTimeOut())
              assignTask()
          }catch{
            case e : Throwable=>logInfo("checker error",e)
          }
        }
      }
    }
    t.setDaemon(true)
    t.setName("Queue Checker")
    t.start()

  }

  startChecker
}


object Queen extends Logging {

  private def run(conf: Configuration) {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(Constants.QUEEN_NAME, conf.get(Constants.QUEEN_ADDR), conf.getInt(Constants.QUEEN_PORT, Constants.QUEEN_PORT_DEFAULT))
    actorSystem.actorOf(
      Props(classOf[Queen]),
      name = Constants.QUEEN_NAME)
    actorSystem.awaitTermination()

  }

  def main(args: Array[String]) {
    val conf = new Configuration
    conf.addResource(Constants.CONFIGFILE_NAME)
    val httpServer = new HttpServer(conf)
    httpServer.start()
    run(conf)
  }


}
