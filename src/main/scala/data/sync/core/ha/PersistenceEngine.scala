
package data.sync.core.ha

import data.sync.common.ClientMessages.SubmitJob

import scala.reflect.ClassTag

trait PersistenceEngine {

  def persist(name: String, obj: Object)

  def unpersist(name: String)

  def read[T: ClassTag](prefix: String): Seq[T]

  final def addJob(jobId:String,app: (String,SubmitJob)): Unit = {
    persist(jobId, app)
  }

  final def removeJob(jobId:String): Unit = {
    unpersist(jobId)
  }
  def close() {}
}

class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}
