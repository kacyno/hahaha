package data.sync.core

import java.util.concurrent.ConcurrentHashMap
import data.sync.common.ClusterMessages.BeeDesc
import net.sf.json.{JSONArray, JSONObject}

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import akka.actor.{Address, ActorRef}

/**
 * Created by hesiyuan on 15/6/23.
 */

object BeeManager {
  var connDic = new ConcurrentHashMap[String,BeeDesc]()
  var addressDic = new ConcurrentHashMap[Address,String]
  def getBee(id:String): BeeDesc = connDic(id)
  def updateBee(bd:BeeDesc)={
    connDic(bd.beeId) = bd
    addressDic(bd.sender.path.address)=bd.beeId
  }
  def removeBee(id:String)={
    addressDic-=connDic(id).sender.path.address
    connDic-=id
  }
  def getBeeByAddress(address:Address):BeeDesc={
    if(addressDic.containsKey(address))
      if(connDic.containsKey(addressDic.get(address)))
        return connDic.get(addressDic.get(address))
    return null
  }
  //提供给web展现
  def allBeesInfo()={
    connDic
  }
  //找出最闲的Bee
  def getMostFreeBee():Option[String]={
    if(connDic.isEmpty)
      None
    else {
      val (beeId, desc) = connDic.reduce((b1, b2) => {
        if ((b1._2.runningWorker.asInstanceOf[Float] / b1._2.totalWorker) < (b2._2.runningWorker.asInstanceOf[Float] / b2._2.totalWorker))
          b1
        else
          b2
      })
      if (desc.totalWorker > desc.runningWorker)
        Some(beeId)
      else
        None
    }
  }
  def freeBee(beeId:String): Unit =synchronized{
    connDic(beeId).runningWorker-=1
  }
  def busyBee(beeId:String): Unit =synchronized{
    connDic(beeId).runningWorker+=1
  }

}
