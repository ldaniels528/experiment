package com.qwery.platform
package codegen.sparksql

import java.io.File
import java.net.URL

/**
  * Compiler Settings
  * @param defaultDB the default database name
  * @param inlineSQL indicates whether to generate inline SQL
  */
case class CompilerSettings(defaultDB: Option[String] = Some("global_temp"),
                            inlineSQL: Option[Boolean] = Some(true)) extends JSONCapability {

  def getDefaultDB: String = defaultDB.getOrElse("global_temp")

  def isInlineSQL: Boolean = inlineSQL.contains(true)

}

/**
  * Compiler Settings Companion
  * @author lawrence.daniels@gmail.com
  */
object CompilerSettings {

  /**
    * Creates a new [[CompilerSettings]] instance with default values
    * @return a new [[CompilerSettings]] instance
    */
  def apply(): CompilerSettings = CompilerSettings(Nil)

  /**
    * Creates a new [[CompilerSettings]] instance with values extracted from the given arguments
    * @param args the given command line arguments
    * @return a new [[CompilerSettings]] instance
    */
  def apply(args: Seq[String]): CompilerSettings = {
    import com.qwery.util.OptionHelper.Implicits.Risky._
    CompilerSettings(
      defaultDB = "global_temp",
      inlineSQL = !args.contains("--native-scala")
    )
  }

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