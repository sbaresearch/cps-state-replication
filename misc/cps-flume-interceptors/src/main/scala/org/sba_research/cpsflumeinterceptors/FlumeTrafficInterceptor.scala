package org.sba_research.cpsflumeinterceptors

import java.util

import org.apache.flume.interceptor.Interceptor
import org.apache.flume.{Context, Event}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

class FlumeTrafficInterceptor extends Interceptor {

  import scala.collection.JavaConversions._

  /**
    * TODO: This should be parsed from AML.
    * Path to AML can be defined in Flume conf.
    */
  var macToTwinNameMap: Map[String, String] = Map(
    "28:63:36:9b:0a:ee" -> "PLC1",
    "08:00:27:37:30:f0" -> "HMI1"
  )

  val logger: Logger = LoggerFactory.getLogger(classOf[FlumeTrafficInterceptor])

  override def initialize(): Unit = {}

  override def intercept(event: Event): Event = {

    /* Cf. https://stackoverflow.com/a/4186090/5107545 */
    class CC[T] {
      def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
    }

    object M extends CC[Map[String, Any]]

    object S extends CC[String]

    val message = new String(event.getBody)
    val twinId = for {
      Some(M(m)) <- List(JSON.parseFull(message))
      M(l) <- m.get("layers")
      M(eth) <- l.get("eth")
      S(srcMac) <- eth.get("eth_eth_src")
      S(twinName) <- macToTwinNameMap.get(srcMac)
    } yield twinName

    /* Set headers with added key element for partitioning. */
    twinId.foreach(id => event.setHeaders(event.getHeaders + ("key" -> id)))
    if (twinId.isEmpty) logger.debug("Event could not be associated with any twin.", event)

    event
  }

  override def intercept(events: util.List[Event]): util.List[Event] = events.map(intercept)

  override def close(): Unit = {}

}

class FlumeTrafficInterceptorBuilder extends Interceptor.Builder {
  override def build(): Interceptor = new FlumeTrafficInterceptor()

  override def configure(context: Context): Unit = {}
}