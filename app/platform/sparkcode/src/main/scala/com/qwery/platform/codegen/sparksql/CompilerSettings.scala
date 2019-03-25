package com.qwery.platform
package codegen.sparksql

import java.io.File
import java.net.URL

/**
  * Compiler Settings
  * @param defaultDB the default database name
  * @param inlineSQL indicates whether to generate inline SQL
  */
case class CompilerSettings(defaultDB: String, inlineSQL: Boolean) extends JSONCapability

/**
  * Compiler Settings Companion
  * @author lawrence.daniels@gmail.com
  */
object CompilerSettings {

  /**
    * Creates a new [[CompilerSettings]] instance with default values
    * @return a new [[CompilerSettings]] instance
    */
  def apply(): CompilerSettings = CompilerSettings(defaultDB = "global_temp", inlineSQL = true)

  /**
    * Creates a new [[CompilerSettings]] instance from the given file
    * @param settingsFile the given settings [[File file]]
    * @return a new [[CompilerSettings]] instance
    */
  def fromFile(settingsFile: File): CompilerSettings = {
    import scala.io.Source
    fromString(Source.fromFile(settingsFile).getLines().mkString)
  }

  /**
    * Creates a new [[CompilerSettings]] instance from the given JSON string
    * @param jsonString the given JSON string
    * @return a new [[CompilerSettings]] instance
    */
  def fromString(jsonString: String): CompilerSettings = {
    import net.liftweb.json
    import net.liftweb.json.DefaultFormats

    implicit val formats: DefaultFormats = DefaultFormats
    json.parse(jsonString).extract[CompilerSettings]
  }

  /**
    * Creates a new [[CompilerSettings]] instance from the given URL
    * @param url the given settings [[URL]]
    * @return a new [[CompilerSettings]] instance
    */
  def fromURL(url: URL): CompilerSettings = {
    import scala.io.Source
    fromString(Source.fromURL(url).getLines().mkString)
  }

}