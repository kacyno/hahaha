package data.sync.common

import scala.annotation.meta.{beanSetter, beanGetter}
import scala.beans.BeanProperty

/**
 * Created by hesiyuan on 15/6/19.
 */
sealed trait ClientMessage extends Serializable

object ClientMessages {

  case class DBInfo(@BeanProperty sql: String,
                    @BeanProperty indexFiled: String,
                    @BeanProperty var tables: Array[String],
                    @BeanProperty db: String,
                    @BeanProperty ip: String,
                    @BeanProperty port: String,
                    @BeanProperty user: String,
                    @BeanProperty pwd: String) extends Serializable

  case class SubmitJob(@BeanProperty priority: Int,
                       @BeanProperty var dbinfos: Array[DBInfo],
                       @BeanProperty taskNum: Int,
                       @BeanProperty targetDir: String) extends ClientMessage

  case class SubmitResult(jobId: String) extends ClientMessage

}
