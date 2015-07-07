package data.sync.core.ha

import akka.serialization.Serialization
import data.sync.common.{Constants, Logging, Configuration}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._
import scala.reflect.ClassTag


class ZooKeeperPersistenceEngine(conf: Configuration, val serialization: Serialization)
  extends PersistenceEngine
  with Logging
{
  val WORKING_DIR = conf.get(Constants.QUEEN_ZK_WORK_DIR,Constants.QUEEN_ZK_WORK_DIR_DEFAULT) + "/master_status"
  val zk: CuratorFramework = CuratorUtil.newClient(conf)

  CuratorUtil.mkdir(zk, WORKING_DIR)


  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  override def read[T: ClassTag](prefix: String) = {
    val file = zk.getChildren.forPath(WORKING_DIR).filter(_.startsWith(prefix))
    file.map(deserializeFromFile[T]).flatten
  }

  override def close() {
    zk.close()
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, serialized)
  }

  def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    try {
      Some(serializer.fromBinary(fileData).asInstanceOf[T])
    } catch {
      case e: Exception => {
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
      }
    }
  }
}
