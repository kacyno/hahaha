package data.sync.core

import java.util.{UUID, Comparator, PriorityQueue}
import data.sync.common.ClusterMessages.{JobInfo, TaskAttemptInfo}
import data.sync.common.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
/**
 * Created by hesiyuan on 15/6/23.
 * 按优先级和提交时间排序
 */
object FIFOScheduler extends Logging{
  //待调度的任务（jobId,priority,submitTime）
  var queue = new PriorityQueue[(String, Int, Long)](10000, new Comparator[(String, Int, Long)] {
    override def compare(o1: (String, Int, Long), o2: (String, Int, Long)): Int = {
      if (o1._2 == o2._2)
        return (o1._3 - o2._3).asInstanceOf[Int]
      else return o2._2-o1._2
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
        val tmp = job.appendTasks //.map(a=>a)
        for (task <- tmp) {
          BeeManager.getMostFreeBee(task.lastErrorBee) match {
            case Some(beeId) =>
              job.status=JobStatus.RUNNING
              task.lastErrorBee=null
              val newAttempt = JobManager.generateAttempt(job,task)
              JobManager.mapBeeAttempt(beeId,newAttempt)
              buffer += ((beeId, newAttempt))
              BeeManager.busyBee(beeId)
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
    logInfo("add job "+job.jobId+" to FIFOScheduler")
  }

  def delJob(job: JobInfo) = {
    queue.remove((job.jobId, job.priority, job.submitTime))
    logInfo("remove job "+job.jobId+" from FIFOScheduler")
  }
}
