package data.sync.core

import java.util
import java.util.Collections
import java.util.concurrent.{ThreadPoolExecutor, Executors}
import akka.actor.{ActorSelection, Props, Actor}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import data.sync.common._
import ClusterMessages._
import scala.collection.JavaConversions._

/**
 * Created by hesiyuan on 15/6/19.
 */
class Bee(queenUrl: String,hostname:String) extends Logging {
  var driver: ActorSelection = null
  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)
  private val executorPool = Executors.newCachedThreadPool(daemonThreadFactoryBuilder.build()).asInstanceOf[ThreadPoolExecutor]
  private val workerSet = Collections.synchronizedSet(new util.HashSet[Worker]) //将正在运行的worker放在这里以便进行统一管理
  startDriverHeartbeater

  class BeeActor extends Actor with ActorLogReceive with Logging {
    override def receiveWithLogging = {
      case RegisteredBee => logInfo("Registered Executor!!")
      case StartTask(tad) => startTask(tad)

      case x: DisassociatedEvent =>
        logWarning(s"Received irrelevant DisassociatedEvent $x")
    }

    override def preStart() {
      logInfo("Connecting to driver: " + queenUrl)
      driver = context.actorSelection(queenUrl)
      driver ! RegisterBee("haha", "8088", 10)
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }
  }

  /*
  开始任务
   */
  private def startTask(tad: TaskAttemptInfo) = synchronized {
    val worker = new Worker(tad, this)
    executorPool.execute(worker.getFetcher)
    executorPool.execute(worker.getSinker)
    workerSet += worker
  }

  /*
  结束任务，包括正常与非正常
   */
  def finishTask(worker: Worker) = synchronized {
    workerSet -= worker
  }

  /*
  向queen进行状态汇报
   */
  def updateStatus() = {

  }

  /*
  周期性的向queen汇报自己的情况
   */
  def startDriverHeartbeater() {
    val interval = 1000l

    val t = new Thread() {
      override def run() {
        while (true) {
          updateStatus()
          Thread.sleep(interval)
        }
      }
    }
    t.setDaemon(true)
    t.setName("Queue Heartbeater")
    t.start()

  }
  def run(): Unit ={
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("Bee", hostname, 8088)
    val hostPort = hostname + ":" + boundPort
    actorSystem.actorOf(
      Props(new BeeActor),
      name = "Bee")
    actorSystem.awaitTermination()
  }
}

object Bee extends Logging {
  def main(args: Array[String]) {
    var queenUrl: String = null
    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--queue-url") :: value :: tail =>
          queenUrl = value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          System.exit(1)
      }
    }
    new Bee("akka.tcp://Queen@localhost:8080/user/queen", "localhost").run()
  }


}