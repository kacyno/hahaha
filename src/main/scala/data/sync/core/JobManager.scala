package data.sync.core

import java.util
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import data.sync.common.ClusterMessages._
import data.sync.common.{HdfsUtil, Logging, Constants}
import data.sync.core.ha.PersistenceEngine
import org.apache.commons.lang.{ArrayUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Shell
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object JobManager extends Logging {
  val hdfs: HdfsUtil = HdfsUtil.getHdfsUtil()
  val MAX_ATTEMPT = 3
  //taskAttemptId到taskAttempt的映射
  private val taskAttemptDic = new ConcurrentHashMap[String, TaskAttemptInfo]
  //taskId到taskAttempt映射
  private val taskDic = new ConcurrentHashMap[String, Array[TaskAttemptInfo]]
  //jobid到jobinfo的映射
  private val jobDic = new ConcurrentHashMap[String, JobInfo]
  //bee->taskAttempt
  private val bee2attempt = new ConcurrentHashMap[String, util.HashSet[String]]
  //taskAttempt->bee
  private val attempt2bee = new ConcurrentHashMap[String, String]
  //taskAttempt->report
  private val attempt2report = new ConcurrentHashMap[String, BeeAttemptReport]

  private var persist: PersistenceEngine = null

  def init(persist: PersistenceEngine): Unit = {
    this.persist = persist
  }

  def printMem(): Unit = {
    println(
      s"""
         |taskAttemptDic: $taskAttemptDic
          |
          |taskDic: $taskDic
          |
          |jobDic: $jobDic
          |
          |bee2attempt: $bee2attempt
          |
          |attempt2bee: $attempt2bee
          |
          |attempt2report: $attempt2report
       """.stripMargin)
  }

  /*
   * 将attempt分给一个bee时调用此方法
   * 1）attempt2bee与bee2attempt中注册
   * 2）taskAttemptDic中注册
   * 3）向report中压入一个原始的汇报
   */
  def mapBeeAttempt(beeId: String, attempt: TaskAttemptInfo): Unit = {
    attempt2bee(attempt.attemptId) = beeId;
    var s = bee2attempt.getOrElse(beeId, new util.HashSet[String]())
    s += attempt.attemptId
    bee2attempt(beeId) = s
    taskAttemptDic(attempt.attemptId) = attempt
    initAttemptReport(beeId, attempt.attemptId)
  }

  /*
   * 生成一个attempt
   * 1)根据task构造出一个attempt
   * 2)将attempt添加到task到attempt的映射中
   * 3)更新job中的task信息
   */
  def generateAttempt(job: JobInfo, task: TaskInfo): TaskAttemptInfo = {
    var attempts = JobManager.getAttempts(task.taskId)
    val attemptPostfix = attempts.length + 1
    val newAttempt = TaskAttemptInfo(task, task.taskId + "-attempt-" + attemptPostfix, new Date().getTime, 0l)
    val attemptsBuffer = ArrayBuffer() ++ attempts
    attemptsBuffer += newAttempt
    attempts = attemptsBuffer.toArray
    JobManager.setAttempts(task.taskId, attempts)
    job.appendTasks -= task
    job.runningTasks += task
    newAttempt
  }

  /*
   * 初始一条假的汇报，用于超时判断
   */
  def initAttemptReport(beeId: String, attemptId: String): Unit = {
    attempt2report(attemptId) = BeeAttemptReport(beeId, attemptId, 0, 0, 0, new Date().getTime, "", TaskAttemptStatus.STARTED)
  }

  /*
   * 将bee不再执行一个attempt时调用此方法
   */
  def removeBeeAttempt(beeId: String, attemptId: String): Unit = {
    if (attempt2bee.containsKey(attemptId))
      attempt2bee -= attemptId
    var s = bee2attempt.getOrElse(beeId, new util.HashSet[String]())
    s -= attemptId

  }


  //对长时间没有状态更新的任务启动并行
  def checkTimeOut(): Boolean = JobManager.synchronized {
    logInfo("Timout Check...")
    var needAssign = false
    val now = System.currentTimeMillis()
    for ((attemptId, bee) <- attempt2bee) {
      //两分钟没有汇报状态的将重新生成一个attempt并行执行,并不会杀死原attempt
      //遍历汇报集合，发现长时间未更新的且并未完成的attempt时找到其对应的task
      //查看该task是否需要进行预测执行

      val report = attempt2report(attemptId)
      if (now - report.time > Constants.TASK_TIMEOUT
        && report.status != TaskAttemptStatus.FAILED
        && report.status != TaskAttemptStatus.FINISHED
        && report.status != TaskAttemptStatus.KILLED) {
        val task = taskAttemptDic(attemptId).taskDesc
        //只有当当前attempt为task最新的attempt时才会进行探测执行，
        if (attemptId == taskDic(task.taskId).last.attemptId) {
          //该task是否在重试范围内
          val job = jobDic(task.jobId)
          if (taskDic(task.taskId).length < MAX_ATTEMPT && !job.appendTasks.contains(task)) {
            // 找出对应task最新的attempt看是否需要再次探测执行
            logInfo("Attempt "+attemptId+" will have a brother!")
            task.lastErrorBee = bee
            job.runningTasks -= task
            job.appendTasks += task
            needAssign = true
          }
        }
      }
    }
    needAssign
  }

  /*
   * 处理bee的汇报，核心方法，做为作业状态切换的依据
   *
   */
  def processReport(beeId: String, report: BeeAttemptReport, queen: Queen): Unit = JobManager.synchronized {
    val apt = taskAttemptDic.getOrElse(report.attemptId, null)

    //当task未结束时才接受汇报
    if (apt != null && !apt.taskDesc.isFinished()) {

      val ar = attempt2report.getOrElse(report.attemptId, null)

      if (ar == null || ar != report) //只对变更的汇报进行更新
        updateReport(report)

      apt.status = report.status

      val task = apt.taskDesc

      task.status = TaskStatus.RUNNING

      val job = jobDic(task.jobId)

      if (report.status == TaskAttemptStatus.FINISHED) {
        logInfo(report.attemptId + " is FINISHED")
        //如果有其它并行运行的attempt,干掉
        for (attempt2kill <- taskDic(task.taskId) if (!attempt2kill.isFinished())) {
          BeeManager.getBee(attempt2bee(attempt2kill.attemptId)).sender ! StopAttempt(attempt2kill.attemptId)
          finishedAttempt(attempt2kill.attemptId, TaskAttemptStatus.KILLED)
        }
        finishedAttempt(report.attemptId, TaskAttemptStatus.FINISHED)

        task.finished(TaskStatus.FINISHED)
        job.appendTasks.remove(task)//可能任务在append中等待重试
        job.runningTasks.remove(task)
        job.finishedTasks += task

        //job运行成功
        if (job.runningTasks.size == 0 && job.appendTasks.size == 0) {
          commitJob(job.jobId, JobStatus.FINISHED)
        }
      } else if (report.status == TaskAttemptStatus.FAILED) {
        logInfo(report.attemptId + " is FAILED")
        finishedAttempt(report.attemptId, TaskAttemptStatus.FAILED)
        //对于明确失败的bee，记录id,下次分配将忽略该bee
        task.lastErrorBee = beeId
        job.runningTasks -= task
        if (taskDic(task.taskId).length == MAX_ATTEMPT
          && taskDic(task.taskId).filter(_.status != TaskAttemptStatus.FAILED).length == 0) {
          //达到最大重试次数，并且已经执行过的attempt全是失败
          task.finished(TaskStatus.FAILED)
          job.failedTasks += task
          killJob(job.jobId)
        } else {
          logInfo("re-assign task " + task.taskId)
          job.appendTasks += task
          queen.assignTask()
        }
      }
    }
  }

  def removeAttemptByBee(beeId: String, queen: Queen): Unit = {
    for (atpId <- bee2attempt.getOrElse(beeId, new util.HashSet[String]())) {
      attempt2bee -= atpId
      val atp = taskAttemptDic(atpId)
      atp.finished(TaskAttemptStatus.FAILED)
      val task = atp.taskDesc
      val job = jobDic(atp.taskDesc.jobId)
      job.runningTasks -= task
      if (taskDic(task.taskId).length >= MAX_ATTEMPT) {
        task.finished(TaskStatus.FAILED)
        job.failedTasks += task
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
  def commitJob(jobId: String, status: JobStatus): Unit = {
    logInfo(jobId + " commit to " + status)
    val job = jobDic(jobId)
    job.finished(status)
    FIFOScheduler.delJob(job)
    if (job.status == JobStatus.FINISHED) {
      try {
        for (task <- job.finishedTasks) {
          for (atpId <- taskDic(task.taskId) if (taskAttemptDic(atpId.attemptId).status == TaskAttemptStatus.FINISHED)) {
            hdfs.rename(new Path(job.targetDir + "tmp/" + atpId.attemptId), new Path(job.targetDir + atpId.attemptId))
          }
        }
        hdfs.delete(new Path(job.targetDir + "tmp/")) //失败的任务可能在删文件时还在操作，只删成功的
        hdfs.createNewFile(new Path(job.targetDir + "_SUCCESS"))
      } catch {
        //IO操作，当报错时捕获，不影响其它后续操作
        case e: Throwable => {
          log.error("commit error!", e)
          job.finished(JobStatus.FAILED)
        }
      }
      //如果作业有配置回调命，执行
      if (StringUtils.isNotEmpty(job.callbackCMD)) {
        val command = job.callbackCMD.split(" ")
        logInfo(job.jobId + " has cmd： " + ArrayUtils.toString(command))
        val exe = new Shell.ShellCommandExecutor(command)
        var exitCode = 0
        try {
          exe.execute()
        } catch {
          case e: Exception => logError("cmd execute failed ", e)
        } finally {
          exitCode = exe.getExitCode()
          logInfo(exitCode.toString)
          logInfo(exe.getOutput())
        }
      }
    }
    clearJob(jobId)
  }

  def finishedAttempt(attemptId: String, status: TaskAttemptStatus): Unit = {
    taskAttemptDic(attemptId).finished(status);
    taskAttemptDic(attemptId).finishTime = new Date().getTime
    val beeId = attempt2bee(attemptId)
    BeeManager.freeBee(beeId)
    removeBeeAttempt(beeId, attemptId);
  }

  /*
   *Job出现错误，杀死job
   * 向所有相关的bee发送杀死job的请求，同时commitjob
   */
  def killJob(jobId: String, status: JobStatus = JobStatus.FAILED): Unit = synchronized {
    logInfo(jobId + " is killed by " + status)
    val job = jobDic(jobId)
    for (task <- (job.runningTasks ++ job.appendTasks)) {
      if (taskDic.containsKey(task.taskId)) {
        for (apt <- taskDic(task.taskId)) {
          if (!apt.isFinished())
            apt.finished(TaskAttemptStatus.KILLED)
          if (attempt2bee.containsKey(apt.attemptId)) {
            //有正运行的attempt,杀掉
            BeeManager.getBee(attempt2bee(apt.attemptId)).sender ! StopAttempt(apt.attemptId)
            finishedAttempt(apt.attemptId, TaskAttemptStatus.KILLED)
          }

        }
      }
      task.finished(TaskStatus.KILLED)
      job.failedTasks += task
    }
    commitJob(jobId, status)
  }

  /*
   * 清理job相关数据,并将其保存在文件中
   */
  private def clearJob(jobId: String): Unit = {
    try {
      //将历史保存
      JobHistory.addJobToHistory(jobId)
      //将job备份清除
      persist.removeJob(jobId)
    } catch {
      case e: Throwable => log.error("clear error!", e)
    }
    //清理数据
    val job = jobDic(jobId)

    val tasks = job.appendTasks ++ job.runningTasks ++ job.finishedTasks ++ job.failedTasks
    for (task <- tasks) {
      if (taskDic.containsKey(task.taskId)) {
        for (attempt <- taskDic(task.taskId)) {
          if (attempt2bee.containsKey(attempt.attemptId))
            removeBeeAttempt(attempt2bee(attempt.attemptId), attempt.attemptId)
          attempt2report -= attempt.attemptId
          taskAttemptDic -= attempt.attemptId
        }
      }
      taskDic -= task.taskId
    }
    jobDic -= jobId
  }


  def getAttemptsByBee(beeId: String): util.Set[(TaskAttemptInfo, BeeAttemptReport)] = {
    val attemptIds = bee2attempt.getOrElse(beeId, new util.HashSet[String]())
    attemptIds.map(id => {
      (taskAttemptDic(id), attempt2report(id))
    })
  }


  def getAttempts(taskId: String): Array[TaskAttemptInfo] = {
    taskDic.getOrElse(taskId, Array[TaskAttemptInfo]())
  }

  def setAttempts(taskId: String, attempts: Array[TaskAttemptInfo]) {
    taskDic(taskId) = attempts
  }

  def getReport(attemptId: String): BeeAttemptReport = {
    attempt2report.getOrElse(attemptId, null)
  }

  def updateReport(report: BeeAttemptReport): Unit = {
    attempt2report(report.attemptId) = report
  }


  def getJob(jobId: String): JobInfo = {
    jobDic.getOrElse(jobId, null)
  }

  def addJob(job: JobInfo): Unit = {
    jobDic(job.jobId) = job
  }

  def allJobs(): java.util.Map[String, JobInfo] = {
    jobDic
  }
}
