package com.github.ldaniels528.qwery.devices

import java.util.UUID

import com.github.ldaniels528.qwery.ops.Scope
import com.github.ldaniels528.qwery.sources.{Statistics, StatisticsGenerator}

/**
  * Represents an I/O device
  * @author lawrence.daniels@gmail.com
  */
trait Device {
  protected val statsGen = new StatisticsGenerator()

  val deviceId: UUID = UUID.randomUUID()

  def close(): Unit

  def getStatistics: Option[Statistics] = statsGen.update(force = true)

  def open(scope: Scope): Unit = statsGen.reset()

}
