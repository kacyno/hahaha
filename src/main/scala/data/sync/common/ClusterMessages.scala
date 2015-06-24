package data.sync.common

import data.sync.core.{TaskStatus, TaskAttemptStatus}

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClusterMessage extends Serializable

object ClusterMessages {

  //Bee注册时使用，要声明自己有多少worker
  case class RegisterBee(beeId: String, hostPort: String, workerNum: Int) extends ClusterMessage

  //注册后Queen的返回
  case object RegisteredBee extends ClusterMessage

  //
  case class TaskInfo(taskId: String,
                      jobId: String,
                      sql: String,
                      ip: String,
                      port: String,
                      user: String,
                      pwd: String,
                      db: String,
                      table: String,
                      targetDir: String,
                      attemptNum: Int,
                      var status:TaskStatus=TaskStatus.STARTED
                       ) extends Serializable

  case class TaskAttemptInfo(taskDesc: TaskInfo,
                             attemptId: String,
                             var status: TaskAttemptStatus = TaskAttemptStatus.STARTED
                              ) extends ClusterMessage

  case class StartTask(tad: TaskAttemptInfo) extends ClusterMessage

  case class BeeAttemptReport(
                               attemptId: String,
                             processNum: Int,
                             time:Long,
                             error: String="",
                             status: TaskAttemptStatus)

  case class StatusUpdate(beeId: String, reports: Array[BeeAttemptReport]) extends ClusterMessage

  case class KillJob(jobId:String) extends ClusterMessage

}
