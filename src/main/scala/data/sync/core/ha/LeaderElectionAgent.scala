
package data.sync.core.ha

trait LeaderElectionAgent {
  val masterActor: LeaderElectable
  def stop() {}
}

trait LeaderElectable {
  def electedLeader()
  def revokedLeadership()
}

class MonarchyLeaderAgent(val masterActor: LeaderElectable)
  extends LeaderElectionAgent {
  masterActor.electedLeader()
}
