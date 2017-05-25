package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper.Risky._

/**
  * Represents the collection of SQL hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(delimiter: Option[String] = None,
                 gzip: Option[Boolean] = None,
                 headers: Option[Boolean] = None,
                 isJson: Option[Boolean] = None,
                 quotedNumbers: Option[Boolean] = None,
                 quotedText: Option[Boolean] = None) {

  def usingFormat(format: String): Hints = {
    format.toUpperCase() match {
      case "CSV" => asCSV
      case "JSON" => asJSON
      case "PSV" => asPSV
      case "TSV" => asTSV
      // TODO support user defined formats
      case _ => this
    }
  }

  def asCSV: Hints = copy(delimiter = ",", headers = true, quotedText = true, quotedNumbers = false)

  def asJSON: Hints = copy(isJson = true)

  def asPSV: Hints = copy(delimiter = "|", headers = true, quotedText = true, quotedNumbers = false)

  def asTSV: Hints = copy(delimiter = "\t", headers = true, quotedText = true, quotedNumbers = false)

  def isEmpty: Boolean = !nonEmpty

  def nonEmpty: Boolean = Seq(delimiter, gzip, headers, isJson, quotedNumbers, quotedText).exists(_.nonEmpty)

}
