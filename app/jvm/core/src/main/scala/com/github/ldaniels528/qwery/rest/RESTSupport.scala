package com.github.ldaniels528.qwery.rest

import akka.actor.Actor
import akka.stream.ActorMaterializer
import com.github.ldaniels528.qwery.actors.ActorSupport
import org.slf4j.LoggerFactory
import play.api.libs.ws.ahc._
import play.api.libs.ws.{BodyWritable, StandaloneWSRequest}

import scala.concurrent.{ExecutionContext, Future}

/**
  * REST Support
  * @author lawrence.daniels@gmail.com
  */
trait RESTSupport extends ActorSupport {
  self: Actor =>

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private implicit val materializer = ActorMaterializer()
  private val ws = StandaloneAhcWSClient()
  protected var debuggingOn = false

  def delete(url: String)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"DELETE $url")
    ws.url(url).delete()
  }

  def get(url: String)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"GET $url")
    ws.url(url).get()
  }

  def patch(url: String)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"PATCH '$url'")
    ws.url(url).execute("PATCH")
  }

  def patch[A: BodyWritable](url: String, body: A)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"PATCH '$url' ~> $body")
    ws.url(url).patch(body)
  }

  def post[A: BodyWritable](url: String, body: A)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"POST '$url' ~> $body")
    ws.url(url)
      .withHttpHeaders("Content-Type" -> "application/json")
      .post(body)
  }

  def put[A: BodyWritable](url: String, body: A)(implicit ec: ExecutionContext): Future[StandaloneWSRequest#Response] = {
    log(s"PUT '$url' ~> $body")
    ws.url(url).put(body)
  }

  protected def log(message: String): Unit = if (debuggingOn) logger.debug(message)

}
