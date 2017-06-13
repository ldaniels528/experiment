package com.github.ldaniels528.transgress.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Slave-like model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait SlaveLike extends js.Object {

  def name: js.UndefOr[String]

  def host: js.UndefOr[String]

  def port: js.UndefOr[String]

  def concurrency: js.UndefOr[Int]

  def maxConcurrency: js.UndefOr[Int]

  def lastUpdated: js.UndefOr[js.Date]

}

/**
  * SlaveLike Companion
  * @author lawrence.daniels@gmail.com
  */
object SlaveLike {

  final implicit class SlaveLikeEnrichment(val slave: SlaveLike) extends AnyVal {

    def getEndpoint: js.UndefOr[String] = for {
      host <- slave.host
      port <- slave.port
    } yield s"$host:$port"

  }

}