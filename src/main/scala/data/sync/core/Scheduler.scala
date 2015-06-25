package data.sync.core

import java.util.{UUID, Comparator, PriorityQueue}
import data.sync.common.ClusterMessages.TaskAttemptInfo
import scala.collection.mutable.ArrayBuffer

/**
 * Created by hesiyuan on 15/6/23.
 * 按优先级和提交时间排序
 */
object FIFOScheduler {
  //待调度的任务（jobId,priority,submitTime）
  var queue = new PriorityQueue[(String, Int, Long)](10000, new Comparator[(String, Int, Long)] {
    override def compare(o1: (String, Int, Long), o2: (String, Int, Long)): Int = {
      if (o1._2 == o2._2)
        return (o1._3 - o2._3).asInstanceOf[Int]
      else return o1._2 - o2._2
    }
  })

  //获得任务分配
  def assigns(): Array[(String, TaskAttemptInfo)] = JobManager.synchronized{
    var flag = true;
    var buffer = ArrayBuffer[(String, TaskAttemptInfo)]()
    val iter = queue.iterator()
    //从queue中依次取出作业进行分配，直到queue中不再有作业，或者没有空闲的bee为止
    while (flag) {
      if (!iter.hasNext)
        flag = false
      else {
        val (jobId, _, _) = iter.next()
        val job = JobManager.getJob(jobId)
        for (task <- job.appendTasks) {
          BeeManager.getMostFreeBee() match {
            case Some(beeId) =>
              job.status=JobStatus.RUNNING
              task.status = TaskStatus.STARTED
              /*
               * 将要把task分到该bee上
               */
              var attempts = JobManager.getAttempts(task.taskId)
              val attemptPostfix = attempts.length+1
              val newAttempt = new TaskAttemptInfo(task, task.taskId+ "-attempt-" + attemptPostfix)
              val attemptsBuffer = ArrayBuffer() ++ attempts
              attemptsBuffer += newAttempt
              attempts = attemptsBuffer.toArray
              JobManager.setAttempts(task.taskId,attempts)

              JobManager.mapBeeAttempt(beeId,newAttempt.attemptId)

              JobManager.updateAttempt(newAttempt)

              buffer += ((beeId, newAttempt))
              job.appendTasks -= task
              job.runningTasks += task
              val bee = BeeManager.getBee(beeId)
              bee.runningWorker += 1
              BeeManager.updateBee(bee)
            case _ => flag = false
          }
        }
      }
    }
    buffer.toArray
  }

  def addJob(job: JobInfo) = {
    job.status = JobStatus.STARTED
    queue.add((job.jobId, job.priority, job.submitTime))
  }

  def delJob(job: JobInfo) = {
    queue.remove((job.jobId, job.priority, job.submitTime))
  }
}
