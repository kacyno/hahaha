package data.sync.core

import akka.actor.{Props, Actor}
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
      logInfo("Registering Executor!!")
      println("queen")
      sender ! ClusterMessages.RegisteredBee
    case SubmitJob(dbinfos) =>submitJob(dbinfos)
    case StatusUpdate(beeId,status) =>updateBees(beeId,status)
    case x: DisassociatedEvent =>
      logWarning(s"Received irrelevant DisassociatedEvent $x")
  }
  /*
  Bee状态的更新
   */
  def updateBees(beeId:String,status:BeeStatus): Unit ={
    //TODO:更新bee各状态
    assignTask();
  }
  /*
   将作业拆分成多个{@link TaskDesc},并封装在Job中放入队列
   */
  def submitJob(dbinfos:Array[DBInfo])={
    //TODO:更新任务
    assignTask()
  }
  /*
  作业拆分
   */
  def splitJob(dbinfos:Array[DBInfo]):List[TaskDesc] = {
    null
  }

  /*
  将任务分配给bee执行
  两个场景触发该方法，1）新任务提交时 2）Bee状态变化时
   */
  def assignTask(): Unit ={

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
