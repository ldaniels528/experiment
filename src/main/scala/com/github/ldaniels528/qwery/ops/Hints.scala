package com.github.ldaniels528.qwery.ops

import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.util.OptionHelper.Risky._

/**
  * Represents the collection of SQL hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(avro: Option[String] = None,
                 delimiter: Option[String] = None,
                 gzip: Option[Boolean] = None,
                 headers: Option[Boolean] = None,
                 isJson: Option[Boolean] = None,
                 properties: Option[JProperties] = None,
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

  def nonEmpty: Boolean = Seq(avro, delimiter, gzip, headers, isJson, properties, quotedNumbers, quotedText).exists(_.nonEmpty)

}
