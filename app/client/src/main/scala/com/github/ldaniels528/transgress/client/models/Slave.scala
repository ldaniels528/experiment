package com.github.ldaniels528.transgress.client.models

import com.github.ldaniels528.transgress.models.SlaveLike

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a slave/worker
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class Slave(var _id: js.UndefOr[String] = js.undefined,
            var name: js.UndefOr[String] = js.undefined,
            var host: UndefOr[String] = js.undefined,
            var port: UndefOr[String] = js.undefined,
            var concurrency: js.UndefOr[Int] = js.undefined,
            var maxConcurrency: js.UndefOr[Int] = js.undefined,
            var lastUpdated: js.UndefOr[js.Date] = js.undefined)
  extends SlaveLike with Expandable {

  var jobs: js.UndefOr[js.Array[Job]] = js.undefined

}