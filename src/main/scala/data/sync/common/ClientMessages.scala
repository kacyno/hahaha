package data.sync.common

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClientMessage extends Serializable
object ClientMessages {
  case class DBInfo(sql:String,tables:Array[String],db:String,ip:String,port:String,user:String,pwd:String) extends Serializable
  case class SubmitJob(dbinfos:Array[DBInfo]) extends ClientMessage
  case class SubmitResult(jobId:String) extends ClientMessage
  case class QueryJobStatus(jobId:String) extends ClientMessage
  case class JobStatus() extends ClientMessage
}
