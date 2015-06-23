package data.sync.core

import data.sync.common.ClusterMessages.TaskAttemptDesc

/**
 * Created by hesiyuan on 15/6/23.
 */
class Scheduler(bm:BeeManager,jm:JobManager) {
  //获得任务分配
  def getAsigns():Array[(BeeId,TaskAttemptDesc)]={
    return Array((null,null))
  }
}
