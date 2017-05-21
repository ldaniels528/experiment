package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper.Risky._

/**
  * Represents the collection of SQL hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(delimiter: Option[String] = None,
                 format: Option[String] = None,
                 headers: Option[Boolean] = None,
                 quotedNumbers: Option[Boolean] = None,
                 quotedText: Option[Boolean] = None) {

  def usingFormat(format: String): Hints = {
    format.toUpperCase() match {
      case "CSV" => copy(delimiter = ",", headers = true, quotedText = true, quotedNumbers = false)
      case "PSV" => copy(delimiter = "|", headers = true, quotedText = true, quotedNumbers = false)
      case "TSV" => copy(delimiter = "\t", headers = true, quotedText = true, quotedNumbers = false)
      // TODO support user defined formats
      case _ => this
    }
  }

  def isEmpty: Boolean = !nonEmpty

  def nonEmpty: Boolean = Seq(delimiter, format, headers, quotedNumbers, quotedText).exists(_.nonEmpty)

}
