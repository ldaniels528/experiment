package com.qwery.platform.sparksql.generator

import java.io.File

/**
  * Application Arguments
  * @param appName          the Spark application name (default: `"Untitled"`)
  * @param appVersion       the version identifier of your application (default: `"1.0"`)
  * @param isClassOnly      indicates whether only a Scala class should be generated (default: `false`)
  * @param defaultDB        the default database (default: `"global_temp"`)
  * @param extendsClass     the optional class the generated class should extend (default: `Serializable`)
  * @param scalaVersion     the Scala version the generated project will use (default: "2.11.12")
  * @param sparkAvroVersion the DataStax Spark-Avro library version (default: "4.0.0")
  * @param sparkCsvVersion  the DataStax Spark-CSV library version (default: "1.5.0")
  * @param sparkVersion     the Apache Spark library version (default: "2.3.3")
  * @param templateClass    the optional template class to use in generating the Spark Job
  * @author lawrence.daniels@gmail.com
  */
case class ApplicationArgs(appName: String,
                           appVersion: String,
                           isClassOnly: Boolean,
                           defaultDB: String,
                           extendsClass: String,
                           scalaVersion: String,
                           sparkAvroVersion: String,
                           sparkCsvVersion: String,
                           sparkVersion: String,
                           templateClass: Option[File])

/**
  * Application Arguments
  * @author lawrence.daniels@gmail.com
  */
object ApplicationArgs {

  /**
    * Creates a new application arguments instance using the given command line arguments
    * @param args the given command line arguments
    * @return the [[ApplicationArgs]]
    */
  def apply(args: Seq[String] = Nil): ApplicationArgs = {
    val mappings = createArgumentsMap(args)
    ApplicationArgs(
      appName = mappings.getOrElse("--app-name", "Untitled"),
      appVersion = mappings.getOrElse("--app-version", "1.0"),
      extendsClass = mappings.getOrElse("--extends-class", "Serializable"),
      isClassOnly = mappings.get("--class-only").exists(v => Seq("t", "true", "y", "yes").contains(v.toLowerCase)),
      defaultDB = mappings.getOrElse("--default-db", "global_temp"),
      scalaVersion = mappings.getOrElse("--scala-version", "2.11.12"),
      sparkAvroVersion = mappings.getOrElse("--spark-avro", "4.0.0"),
      sparkCsvVersion = mappings.getOrElse("--spark-csv", "1.5.0"),
      sparkVersion = mappings.getOrElse("--spark-version", "2.3.3"),
      templateClass = mappings.get("--template-class").map(new File(_))
    )
  }

  private def createArgumentsMap(args: Seq[String]): Map[String, String] = {
    Map(args.toList.sliding(2, 2).toList map {
      case key :: value :: Nil if key.startsWith("--") => key -> value
      case other => throw new IllegalArgumentException(s"Invalid argument specified near '${other.mkString(" ")}'")
    }: _*)
  }

}