package com.github.ldaniels528.qwery.etl.rest

import akka.actor.Actor
import com.github.ldaniels528.qwery.etl.actors.{RegistrationRequest, RegistrationResponse}
import com.github.ldaniels528.qwery.rest.RESTSupport
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.JsonBodyWritables._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Slave REST Client
  * @author lawrence.daniels@gmail.com
  */
trait SlaveClient extends RESTSupport {
  self: Actor =>

  def baseUrl: String

  def registerSlave(request: RegistrationRequest)(implicit ec: ExecutionContext): Future[Option[RegistrationResponse]] = {
    post(s"$baseUrl/api/slave", body = Json.toJson(request))
      .map(_.body[JsValue].asOpt[RegistrationResponse])
  }

}
