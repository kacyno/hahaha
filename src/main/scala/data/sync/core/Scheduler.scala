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
  var queue = new PriorityQueue[(String,Int,Long)](10000,new Comparator[(String,Int,Long)] {
    override def compare(o1: (String, Int, Long), o2: (String, Int, Long)): Int = {
      if(o1._2==o2._2)
        return (o1._3 - o2._3).asInstanceOf[Int]
      else return o1._2-o2._2
    }
  })
  //获得任务分配
  def getAsigns():Array[(String,TaskAttemptInfo)]={
    var flag = true;
    var buffer = ArrayBuffer[(String,TaskAttemptInfo)]()
    while(flag) {
      val (jobId, _, _) = queue.poll()
      if (jobId == null)
        flag = false
      val job = JobManager.getJob(jobId)
      for (task <- job.appendTasks) {
        BeeManager.getMostFreeBee() match {
          case Some(beeId) =>
            buffer += ((beeId, new TaskAttemptInfo(task, job.jobId + "-attempt-" + UUID.randomUUID())))
            job.appendTasks -= task
            job.runningTasks += task
            val bee = BeeManager.getBee(beeId)
            bee.runningWorker += 1
            BeeManager.updateBee(bee)
          case _ => flag = false
        }
      }
    }
    buffer.toArray
  }
  def addJob(job:JobInfo)={
    queue.add((job.jobId,job.priority,job.submitTime))
  }
  def delJob(job:JobInfo)={
    queue.remove((job.jobId,job.priority,job.submitTime))
  }
}
