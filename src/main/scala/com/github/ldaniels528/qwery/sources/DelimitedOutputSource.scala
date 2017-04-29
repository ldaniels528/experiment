package com.github.ldaniels528.qwery.sources

import java.io.{BufferedWriter, File, FileWriter}

/**
  * Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedOutputSource(writer: BufferedWriter, delimiter: String = ",", quoted: Boolean = true)
  extends QueryOutputSource {
  private var headersWritten: Boolean = false

  override def write(data: Seq[(String, Any)]): Unit = {
    if(!headersWritten) {
      headersWritten = true
      val header = data.map(_._1).map(s => '"' + s + '"').mkString(delimiter)
      writer.write(header)
      writer.newLine()
    }

    writer.write(data.map(_._2).map(_.asInstanceOf[AnyRef]).map {
      case n: Number => n.toString
      case x => asString(x)
    } mkString delimiter)
    writer.newLine()
  }

  override def close(): Unit = writer.close()

  private def asString(x: AnyRef) = if(quoted) '"' + x.toString + '"' else x.toString

}

/**
  * Delimited Output Source Companion
  * @author lawrence.daniels@gmail.com
  */
object DelimitedOutputSource {

  def apply(file: File): DelimitedOutputSource = DelimitedOutputSource(new BufferedWriter(new FileWriter(file)))

  def apply(file: String): DelimitedOutputSource = DelimitedOutputSource(new BufferedWriter(new FileWriter(file)))

}