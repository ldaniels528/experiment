package com.github.ldaniels528.qwery.sources

import java.util.UUID

import com.github.ldaniels528.qwery.devices.Device
import com.github.ldaniels528.qwery.ops.Scope

/**
  * Represents an Input or Output Source
  * @author lawrence.daniels@gmail.com
  */
trait IOSource {

  def close(): Unit = device.close()

  def device: Device

  def sourceId: UUID = device.deviceId

  def getStatistics: Option[Statistics] = device.getStatistics

  def open(scope: Scope): Unit = {
    device.open(scope)
    scope.add(sourceId, this)
  }

}
