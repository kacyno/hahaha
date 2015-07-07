package data.sync.core.ha

import data.sync.common.{Constants, Configuration, Logging}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException

import scala.collection.JavaConversions._

object CuratorUtil extends Logging {

  val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  val ZK_SESSION_TIMEOUT_MILLIS = 60000
  val RETRY_WAIT_MILLIS = 5000
  val MAX_RECONNECT_ATTEMPTS = 3

  def newClient(conf:Configuration): CuratorFramework = {
    val ZK_URL = conf.get(Constants.ZK_URL)
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }

  def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
          // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  def deleteRecursive(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path)) {
        zk.delete().forPath(path + "/" + child)
      }
      zk.delete().forPath(path)
    }
  }
}
