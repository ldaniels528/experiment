package com.qwery.platform.sparksql.generator

import java.io.File
import java.text.SimpleDateFormat

import com.qwery.die
import com.qwery.models.Invokable
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.util.Properties

/**
  * Represents a Code Template, which is used to generate class files with custom and/or boiler-plate code.
  * @author lawrence.daniels@gmail.com
  */
class CodeTemplate(templateCode: String) {
  val code = new StringBuilder(templateCode)

  def generate(invokable: Invokable)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    import SparkCodeCompiler.Implicits._
    import com.qwery.util.StringHelper._

    // read the contents of the template file
    val code = new StringBuilder(templateCode)
    val codeBegin = "{{"
    val codeEnd = "}}"
    var lastIndex = 0
    var done = false

    // replace the "{{property}}" tags
    while (!done) {
      val results = for {
        start <- code.indexOfOpt(codeBegin, lastIndex)
        end <- code.indexOfOpt(codeEnd, start).map(_ + codeEnd.length)
        property = code.substring(start + codeBegin.length, end - codeEnd.length).trim
      } yield {
        // determine the replacement value
        val replacement = property match {
          case s if s.toLowerCase startsWith "date:" => new SimpleDateFormat(s.drop(5)).format(new java.util.Date())
          case s if s.toLowerCase startsWith "env:" => Properties.envOrElse(s.drop(4), "")
          case s if s.toLowerCase startsWith "jvm:" => System.getProperty(s.drop(4), "")
          case s if s.toLowerCase startsWith "prop:" => settings.properties.getOrElse(s.drop(5), "")
          case s if s equalsIgnoreCase "appName" => settings.appName
          case s if s equalsIgnoreCase "appVersion" => settings.appVersion
          case s if s equalsIgnoreCase "className" => settings.className
          case s if s equalsIgnoreCase "flow" => invokable.toCode
          case s if s equalsIgnoreCase "inputPath" => settings.inputPath.getCanonicalPath
          case s if s equalsIgnoreCase "outputPath" => settings.outputPath.getCanonicalPath
          case s if s equalsIgnoreCase "packageName" => settings.packageName
          case s if s equalsIgnoreCase "templateFile" => settings.templateFile.map(_.getCanonicalPath).getOrElse("")
          case _ => die(s"Property '$property' is not recognized")
        }

        // replace the tag
        code.replace(start, end, replacement)
        lastIndex = start + replacement.length
      }
      done = results.isEmpty
    }
    code.toString()
  }
}

/**
  * Code Template Companion
  * @author lawrence.daniels@gmail.com
  */
object CodeTemplate {

  /**
    * Return a template from a local file
    * @param templateFile the given local [[File]]
    * @return the [[CodeTemplate]]
    */
  def fromFile(templateFile: File): CodeTemplate = new CodeTemplate(Source.fromFile(templateFile).use(_.getLines().mkString("\n")))

  /**
    * Return a template from a string
    * @param templateCode the given template code string
    * @return the [[CodeTemplate]]
    */
  def fromString(templateCode: String): CodeTemplate = new CodeTemplate(templateCode)

}
