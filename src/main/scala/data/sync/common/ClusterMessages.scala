package data.sync.common

import data.sync.core.{TaskStatus, TaskAttemptStatus}

import scala.beans.BeanProperty

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClusterMessage extends Serializable

object ClusterMessages {

  //Bee注册时使用，要声明自己有多少worker
  case class RegisterBee(workerNum: Int) extends ClusterMessage

  //注册后Queen的返回
  case class RegisteredBee(beeId: String) extends ClusterMessage

  //
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
                      @BeanProperty var status: TaskStatus = TaskStatus.STARTED
                       ) extends Serializable{
    override def equals(obj: scala.Any): Boolean = {
      if(obj==null) return false
      if(!obj.isInstanceOf[TaskInfo]) return false
      this.taskId.equals(obj.asInstanceOf[TaskInfo].taskId)
    }

    override def hashCode(): Int = taskId.hashCode()
  }

  case class TaskAttemptInfo(taskDesc: TaskInfo,
                             attemptId: String,
                             var status: TaskAttemptStatus = TaskAttemptStatus.STARTED
                              ) extends ClusterMessage{
    override def hashCode(): Int = super.hashCode()

    override def equals(obj: scala.Any): Boolean = {
      if(obj==null) return false
      if(!obj.isInstanceOf[TaskAttemptInfo]) return false
      this.attemptId.equals(obj.asInstanceOf[TaskAttemptInfo].attemptId)
    }
  }

  case class StartTask(tad: TaskAttemptInfo) extends ClusterMessage

  case class BeeAttemptReport(
                               attemptId: String,
                               readNum: Long,
                               writeNum: Long,
                               time: Long,
                               error: String = "",
                               status: TaskAttemptStatus)

  case class StatusUpdate(beeId: String, reports: Array[BeeAttemptReport]) extends ClusterMessage

  case class KillJob(jobId: String) extends ClusterMessage

  case class StopAttempt(attemptId: String) extends ClusterMessage

}
