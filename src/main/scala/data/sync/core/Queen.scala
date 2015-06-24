package data.sync.core

import java.util.Date

import akka.actor.{ActorRef, Props, Actor}
import akka.remote.DisassociatedEvent
import data.sync.common.ClientMessages.{DBInfo, SubmitJob}
import data.sync.common._
import data.sync.common.ClusterMessages._
import data.sync.common.Logging
import data.sync.http.server.HttpServer
import scala.collection.mutable

/**
 * Created by hesiyuan on 15/6/19.
 */
class Queen extends Actor with ActorLogReceive with Logging{
  val queue = mutable.PriorityQueue[Int]()
  override def receiveWithLogging = {
    case RegisterBee(id,port,cores) =>
      registerBee(id,port,cores,sender)
      sender ! ClusterMessages.RegisteredBee
    case SubmitJob(dbinfos,taskNum,targetDir) =>submitJob(dbinfos,taskNum,targetDir)
    case StatusUpdate(beeId,reports) =>updateBees(beeId,reports)
    case x: DisassociatedEvent =>
      removeBee(x)
      logWarning(s"Received irrelevant DisassociatedEvent $x")
  }
  /*
   *Bee移除
   *  在BeeManager中移除该Bee
   *  将运行在该Bee上的taskAttempt全部移走
   */
  def removeBee(x:DisassociatedEvent): Unit ={
    val bee = BeeManager.getBeeByAddress(x.remoteAddress)
    BeeManager.removeBee(bee.beeId)
    JobManager.removeAttemptByBee(bee.beeId,this)
    assignTask()
  }
  /*
   *Bee注册
   *1,  记录下bee的信息
   *2,  进行一次任务调度
   */
  def registerBee(id:String,hostPort:String,cores:Int,sender:ActorRef): Unit ={
    logInfo("Registering Executor："+id)
    BeeManager.updateBee(BeeDesc(0,cores,id,hostPort,sender))
    assignTask()
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
  def updateBees(beeId:String,reports:Array[BeeAttemptReport]): Unit ={
    var isWorkerChange = false
    for(report <- reports){
      JobManager.processReport(beeId,report,this)
      if(report.status!=TaskAttemptStatus.RUNNING){//任务结束了
        val bee = BeeManager.getBee(beeId)
        isWorkerChange = true;
        bee.runningWorker-=1
        BeeManager.updateBee(bee) //更新bee的资源信息
      }
    }
    if(isWorkerChange)
      assignTask();
  }
  /*
   *将作业拆分成多个TaskDesc,并封装在Job中放入队列
   */
  def submitJob(dbinfos:Array[DBInfo],num:Int,dir:String):String={
    val jobId = IDGenerator.generatorJobId();
    val tasks = splitJob(dbinfos,num)
    val job = JobInfo(jobId,1,new Date().getTime,dir,dbinfos,tasks,scala.collection.mutable.Set[TaskInfo](),scala.collection.mutable.Set[TaskInfo]())
    JobManager.addJob(job)
    FIFOScheduler.addJob(job)
    assignTask()
    jobId
  }
  /*
   *作业拆分
   */
  def splitJob(dbinfos:Array[DBInfo],num:Int):scala.collection.mutable.Set[TaskInfo] = {
    null
  }

  /*
  将任务分配给bee执行
  两个场景触发该方法，1）新任务提交时 2）Bee上可用的的worker数发生变化
   */
  def assignTask(): Unit ={
    var assigns =  FIFOScheduler.getAsigns()
    for((beeId,tad) <- assigns){
      BeeManager.getBee(beeId).sender ! tad
    }
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
