package data.sync.core

import java.util.concurrent.ConcurrentHashMap

import data.sync.common.ClientMessages.DBInfo
import data.sync.common.ClusterMessages.{KillJob, BeeAttemptReport, TaskInfo, TaskAttemptInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConversions._

/**
 * 维护着Job及task,taskAttempt的相关信息
 * Created by hesiyuan on 15/6/23.
 */
case class JobInfo(jobId: String,
                   priority: Int,
                   submitTime: Long,
                   targetDir: String,
                   info: Array[DBInfo],
                   appendTasks: scala.collection.mutable.Set[TaskInfo],
                   runningTasks: scala.collection.mutable.Set[TaskInfo],
                   finishedTasks: scala.collection.mutable.Set[TaskInfo],
                   var status: JobStatus = JobStatus.STARTED
                    )

object JobManager {
  val fs = FileSystem.get(new Configuration())
  val MAX_ATTEMPT = 3
  //taskAttemptId到taskAttempt的映射
  private val taskAttemptDic = new ConcurrentHashMap[String, TaskAttemptInfo]
  //taskId到taskAttempt映射
  private val taskDic = new ConcurrentHashMap[String, Array[TaskAttemptInfo]]
  //jobid到jobinfo的映射
  private val jobDic = new ConcurrentHashMap[String, JobInfo]
  //bee->taskAttempt
  private val bee2attempt = new ConcurrentHashMap[String, Set[String]]
  //taskAttempt->bee
  private val attempt2bee = new ConcurrentHashMap[String, String]
  //taskAttempt->report
  private val attempt2report = new ConcurrentHashMap[String, BeeAttemptReport]

  def mapBeeAttempt(beeId: String, attemptId: String): Unit = {
    attempt2bee(attemptId) = beeId;
    var s = bee2attempt.getOrElse(beeId, Set[String]())
    s += attemptId
  }

  def removeBeeAttempt(beeId: String, attemptId: String): Unit = {
    attempt2bee -= attemptId
    var s = bee2attempt.getOrElse(beeId, Set[String]())
    s -= attemptId
  }

  def getAttempts(taskId: String): Array[TaskAttemptInfo] = {
    taskDic.getOrElse(taskId, null)
  }

  def getReport(attemptId: String): BeeAttemptReport = {
    attempt2report.getOrElse(attemptId, null)
  }

  def updateReport(report: BeeAttemptReport): Unit = {
    attempt2report(report.attemptId) = report
  }

  def removeReport(attemptId: String): Unit = {
    attempt2report -= attemptId
  }

  def checkTimeOut(): Unit = {
    //TODO: 对于长时间未汇报状态或长时间没有进度的任务处理
  }


  def getJob(jobId: String): JobInfo = {
    jobDic.getOrElse(jobId, null)
  }

  def addJob(job: JobInfo): Unit = {
    jobDic(job.jobId) = job
  }

  def processReport(beeId: String, report: BeeAttemptReport, queen: Queen): Unit = {
    updateReport(report)
    val apt = taskAttemptDic(report.attemptId)
    apt.status = report.status
    val task = apt.taskDesc
    val job = jobDic(task.jobId)
    if (report.status == TaskAttemptStatus.FINSHED) {
      removeBeeAttempt(beeId, report.attemptId)
      job.runningTasks -= task
      job.finishedTasks += task
      //job运行成功
      if (job.runningTasks.size == 0 && job.appendTasks.size == 0)
        commitJob(job.jobId)
    } else if (report.status == TaskAttemptStatus.FAILED) {
      removeBeeAttempt(beeId, report.attemptId)
      job.runningTasks -= task
      if (taskDic(task.taskId).length == MAX_ATTEMPT) {
        killJob(job.jobId)
      } else {
        job.appendTasks += task
      }

    }
  }

  def removeAttemptByBee(beeId: String, queen: Queen): Unit = {
    for (atpId <- bee2attempt(beeId)) {
      attempt2bee -= atpId
      val atp = taskAttemptDic(atpId)
      val task = atp.taskDesc
      val job = jobDic(atp.taskDesc.jobId)
      job.runningTasks -= task
      if (taskDic(task.taskId).length == MAX_ATTEMPT) {
        killJob(job.jobId)
      } else
        job.appendTasks += task
    }
    bee2attempt -= beeId
  }

  /*
   *Job成功运行完成，commit
   * 将job成功的taskAttempt文件移到目标目录下，删除无关的文件
   * 将job从调度队列里移除
   * 将job及相关信息从JobManager里移除
   */
  def commitJob(jobId: String): Unit = {
    val job = jobDic(jobId)
    if (job.status == JobStatus.FINSHED) {
      for (task <- job.finishedTasks) {
        for (atpId <- taskDic(task.taskId) if (taskAttemptDic(atpId.attemptId).status == TaskAttemptStatus.FINSHED)) {
          fs.rename(new Path(job.targetDir + "tmp/" + atpId.attemptId), new Path(job.targetDir + atpId.attemptId))
        }
      }
      FIFOScheduler.delJob(job)
    }
    clearJob(jobId)
  }

  /*
   *Job出现错误，杀死job
   * 向所有相关的bee发送杀死job的请求，同时commitjob
   */
  def killJob(jobId: String): Unit = {
    val job = jobDic(jobId)
    job.status = JobStatus.FAILED
    FIFOScheduler.delJob(job)
    val bees = scala.collection.mutable.Set[String]()
    for (task <- job.runningTasks) {
      for (apt <- taskDic(task.taskId)) {
        if (apt.status != TaskAttemptStatus.FINSHED)
          apt.status = TaskAttemptStatus.KILLED
        if (attempt2bee.contains(apt.attemptId))
          bees += attempt2bee(apt.attemptId)
      }
    }
    bees.foreach(BeeManager.getBee(_).sender ! KillJob(jobId))
    commitJob(jobId)
  }

  /*
   * 清理job相关数据,并将其保存在文件中
   */
  def clearJob(jobId: String): Unit = {
    //将历史保存
    JobHistory.dumpMemJob(jobId)
    //清理数据
    val job = jobDic(jobId)
    fs.delete(new Path(job.targetDir + "tmp/"), true)
    val tasks = job.appendTasks ++ job.runningTasks ++ job.finishedTasks
    for (task <- tasks) {
      for (attempt <- taskDic(task.taskId)) {
        removeBeeAttempt(attempt2bee(attempt.attemptId), attempt.attemptId)
        attempt2report -= attempt.attemptId
        taskAttemptDic -= attempt.attemptId
      }
      taskDic-= task.taskId
    }
    jobDic-=jobId
  }
}
