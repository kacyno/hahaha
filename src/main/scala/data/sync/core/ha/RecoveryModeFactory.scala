package data.sync.core.ha

import akka.serialization.Serialization
import data.sync.common.{Constants, Logging, Configuration}


abstract class StandaloneRecoveryModeFactory(conf: Configuration, serializer: Serialization) {
  def createPersistenceEngine(): PersistenceEngine
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
}

class FileSystemRecoveryModeFactory(conf: Configuration, serializer: Serialization)
  extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {
  val RECOVERY_DIR = conf.get(Constants.QUEEN_RECOVERY_DIR,Constants.QUEEN_RECOVERY_DIR_DEFAULT)

  def createPersistenceEngine() = {
    logInfo("Persisting recovery state to directory: " + RECOVERY_DIR)
    new FileSystemPersistenceEngine(RECOVERY_DIR, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable) = new MonarchyLeaderAgent(master)
}

class ZooKeeperRecoveryModeFactory(conf: Configuration, serializer: Serialization)
  extends StandaloneRecoveryModeFactory(conf, serializer) {
  def createPersistenceEngine() = new ZooKeeperPersistenceEngine(conf, serializer)

  def createLeaderElectionAgent(master: LeaderElectable) =
    new ZooKeeperLeaderElectionAgent(master, conf)
}
