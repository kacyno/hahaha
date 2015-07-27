package data.sync.common

/**
 * Created by hesiyuan on 15/6/19.
 */


import java.net.BindException



import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{Address, ActorRef, ActorSystem, ExtendedActorSystem}
import akka.pattern.ask

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}


/**
 * Various utility classes for working with Akka.
 */
object AkkaUtils extends Logging {


  def createActorSystem(
                         name: String,
                         host: String,
                         port: Int
                         ): (ActorSystem, Int) = {
    val startService: Int => (ActorSystem, Int) = { actualPort =>
      doCreateActorSystem(name, host, actualPort)
    }
    startServiceOnPort(port, startService, name)
  }

  private def doCreateActorSystem(
                                   name: String,
                                   host: String,
                                   port: Int

                                   ): (ActorSystem, Int) = {

    val akkaThreads = 1000
    val akkaBatchSize = 15
    val akkaTimeout = 3
    val akkaFrameSize = maxFrameSizeBytes()
    val akkaLogLifecycleEvents = true
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = "off" //if (conf.getBoolean("spark.akka.logAkkaConfig", false)) "on" else "off"

    val akkaHeartBeatPauses = 600
    val akkaHeartBeatInterval = 10

    //    val secretKey = securityManager.getSecretKey()
    //    val isAuthOn = securityManager.isAuthenticationEnabled()
    //    if (isAuthOn && secretKey == null) {
    //      throw new Exception("Secret key is null with authentication on")
    //    }
    val requireCookie = "off" //if (isAuthOn) "on" else "off"
    val secureCookie = "" //if (isAuthOn) secretKey else ""
    logDebug(s"In createActorSystem, requireCookie is: $requireCookie")

    //    val akkaSslConfig = securityManager.akkaSSLOptions.createAkkaConfig
    //      .getOrElse(ConfigFactory.empty())


    //
    val akkaConf = ConfigFactory.parseMap(scala.collection.mutable.Map[String, String]())
      .withFallback(ConfigFactory.parseString(
      s"""
         |akka.daemonic = on
         |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
         |akka.stdout-loglevel = "DEBUG"
         |akka.loglevel = "DEBUG"
         |akka.jvm-exit-on-fatal-error = off
         |akka.remote.require-cookie = "$requireCookie"
         |akka.remote.secure-cookie = "$secureCookie"
         |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
         |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = $port
         |akka.remote.netty.tcp.tcp-nodelay = on
         |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
         |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
         |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
         |akka.actor.default-dispatcher.throughput = $akkaBatchSize
         |akka.log-config-on-start = $logAkkaConfig
         |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
         |akka.log-dead-letters = $lifecycleEvents
         |akka.log-dead-letters-during-shutdown = $lifecycleEvents
         |akka.remote.retry-gate-closed-for = 0.5 s
         |akka.remote.use-passive-connections = on
         |akka.actor.debug.fsm = on
         |akka.actor.debug.event-stream = on
         |akka.actor.debug.unhandled = on
         |akka.actor.debug.router-misconfiguration = on
         |akka.actor.debug.receive = on
         |akka.actor.debug.autoreceive = on
         |akka.actor.debug.lifecycle = on
      """.stripMargin))

    val actorSystem = ActorSystem(name, akkaConf)
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  /** Returns the default Spark timeout to use for Akka ask operations. */
  def askTimeout(): FiniteDuration = {
    Duration.create(30, "seconds")
  }

  /** Returns the default Spark timeout to use for Akka remote actor lookup. */
  def lookupTimeout(): FiniteDuration = {
    Duration.create(30, "seconds")
  }

  private val AKKA_MAX_FRAME_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max frame size for Akka messages in bytes. */
  def maxFrameSizeBytes(): Int = {
    10 * 1024 * 1024
  }

  /** Space reserved for extra data in an Akka message besides serialized task or task result. */
  val reservedSizeBytes = 200 * 1024

  /** Returns the configured number of times to retry connecting */
  def numRetries(): Int = {
    3
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(): Int = {
    3000
  }

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  def askWithReply[T](
                       message: Any,
                       actor: ActorRef,
                       timeout: FiniteDuration): T = {
    askWithReply[T](message, actor, maxAttempts = 1, retryInterval = Int.MaxValue, timeout)
  }

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails even after the specified number of retries.
   */
  def askWithReply[T](
                       message: Any,
                       actor: ActorRef,
                       maxAttempts: Int,
                       retryInterval: Int,
                       timeout: FiniteDuration): T = {
    // TODO: Consider removing multiple attempts
    if (actor == null) {
      throw new Exception(s"Error sending message [message = $message]" +
        " as actor is null ")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxAttempts) {
      attempts += 1
      try {
        val future = actor.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new Exception("Actor returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }
      Thread.sleep(retryInterval)
    }

    throw new Exception(
      s"Error sending message [message = $message]", lastException)
  }



  def protocol(actorSystem: ActorSystem): String = {
    val akkaConf = actorSystem.settings.config
    val sslProp = "akka.remote.netty.tcp.enable-ssl"
    protocol(akkaConf.hasPath(sslProp) && akkaConf.getBoolean(sslProp))
  }

  def protocol(ssl: Boolean = false): String = {
    if (ssl) {
      "akka.ssl.tcp"
    } else {
      "akka.tcp"
    }
  }

  def address(
               protocol: String,
               systemName: String,
               host: String,
               port: Any,
               actorName: String): String = {
    s"$protocol://$systemName@$host:$port/user/$actorName"
  }

  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             serviceName: String = ""): (T, Int) = {
    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = 3
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new Exception(s"Failed to start service$serviceString on port $startPort")
  }
  def extractAddressFromUrl(sparkUrl: String): Address = {
    val uri = new java.net.URI(sparkUrl)
    val scheme = uri.getScheme
    val host = uri.getHost
    val port = uri.getPort
    val user = uri.getUserInfo
    Address(scheme,user,host,port)
  }
  def main (args: Array[String]) {
    println(extractAddressFromUrl("akka.tcp://queen@localhost:43935/user/queen"))
  }
}

