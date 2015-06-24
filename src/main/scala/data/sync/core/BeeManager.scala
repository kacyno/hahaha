package data.sync.core

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import akka.actor.{Address, ActorRef}

/**
 * Created by hesiyuan on 15/6/23.
 */
case class BeeDesc(var runningWorker:Int,var tatolWorker:Int,beeId:String,hostPort:String,sender:ActorRef  )
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
    return connDic(addressDic(address))
  }
  //找出最闲的Bee
  def getMostFreeBee():Option[String]={

    val (beeId,desc) = connDic.reduce((b1,b2)=>{
          if((b1._2.runningWorker/b1._2.tatolWorker)<(b2._2.runningWorker/b2._2.tatolWorker))
            b1
          else
            b2
        })
    if(desc.tatolWorker>desc.runningWorker)
      Some(beeId)
    else
      None
  }
}
