package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Hints

/**
  * Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait InputSourceFactory extends IOSourceFactory {

  def apply(path: String, hints: Option[Hints]): Option[InputSource]

}
