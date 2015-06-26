package data.sync.core

import data.sync.common.ClientMessages.DBInfo
import data.sync.common.ClusterMessages.TaskInfo

/**
 * Created by hesiyuan on 15/6/24.
 */
trait Splitter {
  def split(jobId:String,dbinfos:Array[DBInfo],num:Int,dir:String):java.util.Set[TaskInfo]
}
