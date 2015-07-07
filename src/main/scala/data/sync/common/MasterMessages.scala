
package data.sync.common

sealed trait MasterMessages extends Serializable

object MasterMessages {


  case object ElectedLeader

  case object RevokedLeadership

  case object CompleteRecovery


}
