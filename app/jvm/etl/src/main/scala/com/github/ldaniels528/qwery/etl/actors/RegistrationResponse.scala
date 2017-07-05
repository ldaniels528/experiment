package com.github.ldaniels528.qwery.etl.actors

import java.util.Date

import play.api.libs.json.Json

/**
  * Represents a Registration Response
  * @author lawrence.daniels@gmail.com
  */
case class RegistrationResponse(_id: Option[String],
                                name: Option[String],
                                host: Option[String],
                                port: Option[String],
                                lastUpdated: Option[Date],
                                maxConcurrency: Option[Int],
                                concurrency: Option[Int])

/**
  * Registration Response
  * @author lawrence.daniels@gmail.com
  */
object RegistrationResponse {

  implicit val registrationResponseFormat = Json.format[RegistrationResponse]

}
