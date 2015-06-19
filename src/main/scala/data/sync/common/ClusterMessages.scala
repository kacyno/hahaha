package data.sync.common

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClusterMessage extends Serializable
object ClusterMessages {
  case class RegisterBee(beeId: String,hostPort: String,cores: Int)extends ClusterMessage
  case object RegisteredBee extends ClusterMessage
  case class TaskDesc(sql:String,connStr:String,db:String,table:String) extends Serializable
}
