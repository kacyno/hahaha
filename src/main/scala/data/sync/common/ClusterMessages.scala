package data.sync.common


import java.util.Date

import akka.actor.ActorRef
import data.sync.common.ClientMessages.DBInfo
import data.sync.core.{TaskAttemptStatus, JobStatus, TaskStatus}

import scala.beans.BeanProperty

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClusterMessage extends Serializable

object ClusterMessages {

  //任务信息
  case class TaskInfo(@BeanProperty taskId: String,
                      @BeanProperty jobId: String,
                      @BeanProperty sql: String,
                      @BeanProperty ip: String,
                      @BeanProperty port: String,
                      @BeanProperty user: String,
                      @BeanProperty pwd: String,
                      @BeanProperty db: String,
                      @BeanProperty table: String,
                      @BeanProperty targetDir: String,
                      @BeanProperty var startTime: Long,
                      @BeanProperty var finishTime: Long,
                      @BeanProperty var status: TaskStatus = TaskStatus.STARTED,
                      @BeanProperty var lastErrorBee:String = ""
                       ) extends Serializable {
    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) return false
      if (!obj.isInstanceOf[TaskInfo]) return false
      this.taskId.equals(obj.asInstanceOf[TaskInfo].taskId)
    }

    override def hashCode(): Int = taskId.hashCode()

    def isFinished(): Boolean = status == TaskStatus.FAILED || status == TaskStatus.FINISHED ||status==TaskStatus.KILLED

    def finished(status:TaskStatus): Unit ={
      if(status == TaskStatus.FAILED || status == TaskStatus.FINISHED || status==TaskStatus.KILLED)
      finishTime = new Date().getTime
      this.status = status
    }
  }

  //attempt信息

  case class TaskAttemptInfo(taskDesc: TaskInfo,
                             attemptId: String,
                             var startTime: Long,
                             var finishTime: Long,
                             var status: TaskAttemptStatus = TaskAttemptStatus.STARTED
                              ) extends ClusterMessage {
    override def hashCode(): Int = attemptId.hashCode

    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) return false
      if (!obj.isInstanceOf[TaskAttemptInfo]) return false
      this.attemptId.equals(obj.asInstanceOf[TaskAttemptInfo].attemptId)
    }
    def isFinished(): Boolean = status == TaskAttemptStatus.FAILED || status == TaskAttemptStatus.FINISHED ||status==TaskAttemptStatus.KILLED
    def finished(status:TaskAttemptStatus): Unit ={
      if(status == TaskAttemptStatus.FAILED || status == TaskAttemptStatus.FINISHED||status==TaskAttemptStatus.KILLED)
        finishTime = new Date().getTime
      this.status = status
    }
  }

  /**
   * 维护着Job及task,taskAttempt的相关信息
   * Created by hesiyuan on 15/6/23.
   */
  case class JobInfo(@BeanProperty jobId: String,
                     @BeanProperty dbinfos: Array[DBInfo],
                     @BeanProperty priority: Int,
                     @BeanProperty var submitTime: Long,
                     @BeanProperty var finishTime: Long,
                     @BeanProperty targetDir: String,
                     @BeanProperty info: Array[DBInfo],
                     @BeanProperty appendTasks: java.util.Set[TaskInfo],
                     @BeanProperty runningTasks: java.util.Set[TaskInfo],
                     @BeanProperty finishedTasks: java.util.Set[TaskInfo],
                     @BeanProperty failedTasks:java.util.Set[TaskInfo],
                     @BeanProperty callbackCMD: String,
                     @BeanProperty notifyUrl:String,
                     @BeanProperty user:String,
                     @BeanProperty jobName:String,
                     @BeanProperty var status: JobStatus = JobStatus.SUBMITED
                      ) {
    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) return false
      if (!obj.isInstanceOf[JobInfo]) return false
      this.jobId.equals(obj.asInstanceOf[JobInfo].jobId)
    }
    override def hashCode(): Int = jobId.hashCode()
    def finished(status:JobStatus): Unit ={
      if(status == JobStatus.FAILED || status == JobStatus.FINISHED||status==JobStatus.KILLED)
        finishTime = new Date().getTime
      this.status = status
    }
  }

  //bee上attempt执行信息的汇报
  case class BeeAttemptReport(
                               beeId:String,
                               attemptId: String,
                               readNum: Long,
                               writeNum: Long,
                               bufferSize:Long,
                               time: Long,
                               error: String = "",
                               status: TaskAttemptStatus) {
    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) return false
      if (!obj.isInstanceOf[BeeAttemptReport]) return false
      val other = obj.asInstanceOf[BeeAttemptReport]
      attemptId == other.attemptId && readNum == other.readNum && writeNum == other.writeNum && status == other.status
    }

    override def hashCode(): Int = attemptId.hashCode ^ readNum.hashCode & writeNum.hashCode ^ status.hashCode
  }



  //启动task
  case class StartTask(tad: TaskAttemptInfo) extends ClusterMessage
  //Bee注册时使用，要声明自己有多少worker
  case class RegisterBee(workerNum: Int) extends ClusterMessage
  //注册后Queen的返回
  case class RegisteredBee(beeId: String) extends ClusterMessage
  //状态更新
  case class StatusUpdate(beeId: String, reports: Array[BeeAttemptReport]) extends ClusterMessage
  //杀死job
  case class KillJob(jobId: String) extends ClusterMessage
  //终止attempt
  case class StopAttempt(attemptId: String) extends ClusterMessage
  //bee相关信息描述
  case class BeeDesc(@BeanProperty var runningWorker: Int, @BeanProperty var totalWorker: Int, @BeanProperty beeId: String, sender: ActorRef)

}
