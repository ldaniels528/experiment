package com.qwery.database.server

import com.qwery.database.{BlockDevice, SHORT_BYTES}

case class DeviceRef(name: String) {

  def toDevice(implicit ctx: RuntimeContext): BlockDevice = ctx.lookupDeviceByName(name)

  val length: Int = name.length + SHORT_BYTES

}
