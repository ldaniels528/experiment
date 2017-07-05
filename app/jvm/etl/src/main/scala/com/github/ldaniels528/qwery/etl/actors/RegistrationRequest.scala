package com.github.ldaniels528.qwery.etl.actors

import play.api.libs.json._

/**
  * Represents a Registration Request
  * @author lawrence.daniels@gmail.com
  */
case class RegistrationRequest(name: String, host: String, port: String, maxConcurrency: Int, concurrency: Int = 0)

/**
  * Registration Request
  * @author lawrence.daniels@gmail.com
  */
object RegistrationRequest {

  implicit val registrationRequestFormat = Json.format[RegistrationRequest]

}