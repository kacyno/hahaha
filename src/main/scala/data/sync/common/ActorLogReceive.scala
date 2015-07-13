package data.sync.common

import akka.actor.Actor
import org.slf4j.Logger

/**
 * Created by hesiyuan on 15/6/19.
 */
trait ActorLogReceive {
  self: Actor =>

  override def receive: Actor.Receive = new Actor.Receive {

    private val _receiveWithLogging = receiveWithLogging

    override def isDefinedAt(o: Any): Boolean = _receiveWithLogging.isDefinedAt(o)

    override def apply(o: Any): Unit = {
      if (log.isDebugEnabled) {
        log.debug(s"[actor] received message $o from ${self.sender}")
      }
      val start = System.nanoTime
      try {
        _receiveWithLogging.apply(o)
      }catch{
        case e:Throwable=>log.error("actor error",e)
      }
      val timeTaken = (System.nanoTime - start).toDouble / 1000000
      if (log.isDebugEnabled) {
        log.debug(s"[actor] handled message ($timeTaken ms) $o from ${self.sender}")
      }
    }
  }

  def receiveWithLogging: Actor.Receive
  protected def log: Logger
}
