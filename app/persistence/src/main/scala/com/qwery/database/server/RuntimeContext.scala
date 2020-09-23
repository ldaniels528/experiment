package com.qwery.database.server

import com.qwery.database.{BlockDevice, ROWID}

import scala.collection.concurrent.TrieMap

/**
 * Represents the runtime state of a database session
 */
class RuntimeContext() {
  private val devices = TrieMap[String, TableFile]()
  var defaultDevice: Option[BlockDevice] = None
  var defaultOffset: Option[ROWID] = None

  def lookupDeviceByName(name: String): BlockDevice = devices.getOrElseUpdate(name, TableFile(name)).device

}
