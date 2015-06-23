package data.sync.core

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import akka.actor.ActorRef

/**
 * Created by hesiyuan on 15/6/23.
 */
case class BeeId(val host:String,val port:Int) extends Serializable
case class BeeDesc(var runningWorker:Int,var tatolWorker:Int,id:BeeId)
object BeeManager {
  //按空闲worker与总worker的比值排序
  var connDic = new ConcurrentHashMap[BeeId,ActorRef]()
  def getActorRef(id:BeeId): ActorRef = connDic(id)
  def registerBee(bd:BeeDesc,sender:ActorRef)={

  }
  def updateBee(bd:BeeDesc)={

  }
  def removeBee(id:BeeId)={

  }
}
