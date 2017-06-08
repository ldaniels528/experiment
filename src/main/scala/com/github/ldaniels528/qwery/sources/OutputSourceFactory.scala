package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.OutputDevice
import com.github.ldaniels528.qwery.ops.Hints

/**
  * Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait OutputSourceFactory extends IOSourceFactory {

  def findOutputSource(device: OutputDevice, append: Boolean, hints: Option[Hints]): Option[OutputSource]

}
