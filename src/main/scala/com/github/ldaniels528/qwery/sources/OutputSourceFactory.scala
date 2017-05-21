package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Hints

/**
  * Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait OutputSourceFactory extends IOSourceFactory {

  def apply(uri: String, append: Boolean, hints: Hints): Option[OutputSource]

}
