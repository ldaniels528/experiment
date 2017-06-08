package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.InputDevice
import com.github.ldaniels528.qwery.ops.Hints

/**
  * Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait InputSourceFactory extends IOSourceFactory {

  def findInputSource(device: InputDevice, hints: Option[Hints]): Option[InputSource]

}
