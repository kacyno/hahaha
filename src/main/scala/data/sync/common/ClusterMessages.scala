package data.sync.common

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClusterMessage extends Serializable
object ClusterMessages {
  case class BeeStatus() extends ClusterMessage
  case class RegisterBee(beeId: String,hostPort: String,cores: Int)extends ClusterMessage
  case object RegisteredBee extends ClusterMessage
  case class TaskDesc(sql:String,ip:String,port:String,user:String,pwd:String,db:String,table:String,targetDir:String,attemptNum:Int) extends Serializable
  case class TaskAttemptDesc(taskDesc:TaskDesc,attemptId:String) extends ClusterMessage
  case class StartTask(tad:TaskAttemptDesc) extends ClusterMessage
  case class StatusUpdate(beeId:String,status:BeeStatus) extends ClusterMessage
}
