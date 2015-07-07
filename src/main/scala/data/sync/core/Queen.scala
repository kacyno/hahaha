package data.sync.core

import java.util.Date
import akka.actor.{ActorRef, Props, Actor}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.{Serialization, SerializationExtension}
import data.sync.common.ClientMessages._
import data.sync.common.MasterMessages.{CompleteRecovery, RevokedLeadership, ElectedLeader}
import data.sync.common._
import data.sync.common.ClusterMessages._
import data.sync.common.Logging
import data.sync.core.ha._
import data.sync.http.server.HttpServer
import net.sf.json.JSONObject

/**
 * Created by hesiyuan on 15/6/19.
 */
class Queen(conf:Configuration,queenUrl:String) extends Actor with ActorLogReceive with Logging with LeaderElectable{



  var persistenceEngine: PersistenceEngine = _
  val RECOVERY_MODE = conf.get(Constants.RECOVERY_MODE, Constants.RECOVERY_MODE_DEFAULT)
  var leaderElectionAgent: LeaderElectionAgent = _
  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Class.forName(conf.get(Constants.CUSTOM_RECOVERY_FACTORY))
        val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
          .newInstance(conf, SerializationExtension(context.system))
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
    JobManager.init(persistenceEngine)
  }

  override def receiveWithLogging = {
    case RegisterBee(cores) =>
      if (Queen.state == RecoveryState.STANDBY) {
        // ignore, don't send response
      }else {
        val beeId = registerBee(cores, sender)
        sender ! ClusterMessages.RegisteredBee(queenUrl,beeId)
        assignTask()
      }
    case RegisterClient =>
      if (Queen.state == RecoveryState.STANDBY) {
        // ignore, don't send response
      }else {
        sender ! RegisteredClient(queenUrl)
      }
    case job @ SubmitJob(priority, dbinfos, taskNum, cmd,url,user,jobName,targetDir) =>
      logInfo(
        """
          |Submit job from %s
          |Job desc:
          |%s
        """.stripMargin.format(sender.path.address,JSONObject.fromObject(job).toString()))
      try {
        val jobId = submitJob(job)
        //保存作业，用于恢复
        persistenceEngine.addJob(jobId,(jobId,job))
        sender ! SubmitResult(jobId)
      }catch{
        case e:Exception=>
          logError("submit job error",e)
          sender!SubmitResult(e.getMessage)
      }
    case StatusUpdate(beeId, reports) => updateBees(beeId, reports)
    case data.sync.common.ClientMessages.KillJob(jobId)=>
      try {
        JobManager.killJob(jobId, JobStatus.KILLED)
      }catch{
        case e:Exception=>
          logError("kill error "+jobId,e)
          sender ! KillJobResult(e.getMessage)
      }
      sender ! KillJobResult(s"killing job $jobId")
    case x: DisassociatedEvent =>
      removeBee(x)
      logWarning(s"Received irrelevant DisassociatedEvent $x")

    case ElectedLeader => {
      val jobs = persistenceEngine.read[(String,SubmitJob)]("")
      Queen.state = if (jobs.isEmpty ) {
        RecoveryState.ACTIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + Queen.state)
      if (Queen.state == RecoveryState.RECOVERING) {
        beginRecovery(jobs)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }
    case other =>
      logInfo(other.toString)
  }
  def beginRecovery(jobs:Seq[(String,SubmitJob)]): Unit ={
    Queen.state = RecoveryState.RECOVERING
    jobs.foreach(e=>submitJob(e._2,e._1))
    self ! CompleteRecovery
  }
  def completeRecovery(): Unit = {
    Queen.state = RecoveryState.COMPLETING_RECOVERY
    //nothing to do
    Queen.state = RecoveryState.ACTIVE
  }

  /*
   *Bee移除
   *  在BeeManager中移除该Bee
   *  将运行在该Bee上的taskAttempt全部移走
   */
  def removeBee(x: DisassociatedEvent): Unit = {
    if (x.remoteAddress.toString.indexOf("bee") != -1) {
      val bee = BeeManager.getBeeByAddress(x.remoteAddress)
      if (bee != null) {
        logInfo("Remove bee "+bee.beeId)
        BeeManager.removeBee(bee.beeId)
        JobManager.removeAttemptByBee(bee.beeId, this)
        assignTask()
      }
    }
  }

  /*
   *Bee注册
   *1,  记录下bee的信息
   *2,  进行一次任务调度
   */
  def registerBee(cores: Int, sender: ActorRef): String = {
    val beeId = sender.path.address.hostPort
    logInfo("Registering Bee：" + beeId)
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
  def submitJob(submit:SubmitJob,jd:String=null):String={
    var jobId = jd
    if(jd==null)
      jobId = IDGenerator.generatorJobId();
    val tasks = SimpleSplitter.split(jobId, submit.dbinfos,submit.taskNum , submit.targetDir)
    val job = JobInfo(jobId,
      submit.dbinfos,
      submit.priority,
      new Date().getTime,
      0l,
      submit.targetDir,
      submit.dbinfos,
      tasks,
      new java.util.HashSet[TaskInfo](),
      new java.util.HashSet[TaskInfo](),
      new java.util.HashSet[TaskInfo](),
      submit.callbackCMD,
      submit.url,
      submit.user,
      submit.jobName,
      JobStatus.SUBMITED
    )
    JobManager.addJob(job)
    //加入调度
    FIFOScheduler.addJob(job)
    Notifier.notifyJob(job)
    assignTask()
    //作业提交后进行一次通知
    jobId
  }

  /*
  将任务分配给bee执行
  两个场景触发该方法，1）任务需要调度时 2）可用的的worker数发生变化时
   */
  def assignTask(): Unit = {
    val assigns = FIFOScheduler.assigns
    for ((beeId, tad) <- assigns) {
      logInfo("send task: " + tad.attemptId + " to bee:" + beeId)
      BeeManager.getBee(beeId).sender ! StartTask(tad)
      //当任发送给Bee后即认为任务开始
      if(tad.taskDesc.startTime == 0)
        tad.taskDesc.startTime = new Date().getTime
      //attempt开始时间
      tad.startTime = new Date().getTime;
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
          } catch {
            case e: Throwable => logInfo("Checker error", e)
          }
        }
      }
    }
    t.setDaemon(true)
    t.setName("Queue Checker")
    t.start()

  }

  startChecker

  override def electedLeader() {
    self ! ElectedLeader
  }

  override def revokedLeadership() {
    self ! RevokedLeadership
  }
}


object Queen extends Logging {
  var state = RecoveryState.STANDBY
  var conf = new Configuration()
  conf.addResource(Constants.CONFIGFILE_NAME)
  private def run(conf: Configuration) {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(Constants.QUEEN_NAME, conf.get(Constants.QUEEN_ADDR), conf.getInt(Constants.QUEEN_PORT, Constants.QUEEN_PORT_DEFAULT))
    val queenUrl = "akka.tcp://%s@%s:%d/user/%s".format(Constants.QUEEN_NAME,conf.get(Constants.QUEEN_ADDR),boundPort,Constants.QUEEN_NAME)
    actorSystem.actorOf(
      Props(classOf[Queen],conf,queenUrl),
      name = Constants.QUEEN_NAME)
    Bee.printBee()
    actorSystem.awaitTermination()

  }

  def main(args: Array[String]) {
    val httpServer = new HttpServer(conf)
    httpServer.start()
    JobHistory.init()
    Notifier.start()
    run(conf)
  }


}
