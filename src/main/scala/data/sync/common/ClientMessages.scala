package data.sync.common

import scala.annotation.meta.{beanSetter, beanGetter}
import scala.beans.BeanProperty

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClientMessage extends Serializable

object ClientMessages {

  case class DBInfo(@BeanProperty var sql: String,
                    @BeanProperty indexFiled: String,
                    @BeanProperty var tables: Array[String],
                    @BeanProperty db: String,
                    @BeanProperty ip: String,
                    @BeanProperty port: String,
                    @BeanProperty user: String,
                    @BeanProperty pwd: String,
                    @BeanProperty var needFix:Boolean=true
                     ) extends ClientMessage

  case class SubmitJob(@BeanProperty var priority: Int,
                       @BeanProperty var dbinfos: Array[DBInfo],
                       @BeanProperty var taskNum: Int,
                       @BeanProperty callbackCMD: String,
                       @BeanProperty url:String,
                       @BeanProperty user:String,
                       @BeanProperty var jobName:String,
                       @BeanProperty var targetDir: String,
                       @BeanProperty var codec:String ) extends ClientMessage
  case object RegisterClient extends ClientMessage
  case class RegisteredClient(queenUrl:String) extends ClientMessage
  case class KillJob(jobId:String) extends ClientMessage
  case class SubmitResult(jobId: String) extends ClientMessage
  case class KillJobResult(message:String) extends ClientMessage

}
