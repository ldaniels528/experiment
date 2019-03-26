package com.qwery.platform
package codegen.sparksql

/**
  * Application Arguments
  * @author lawrence.daniels@gmail.com
  */
case class ApplicationArgs(appName: String,
                           appVersion: String,
                           scalaVersion: String,
                           sparkAvroVersion: String,
                           sparkCsvVersion: String,
                           sparkVersion: String)

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
  def apply(args: Seq[String]): ApplicationArgs = {
    val mappings = createArgumentsMap(args)
    ApplicationArgs(
      appName = mappings.getOrElse("--app-name", "Untitled"),
      appVersion = mappings.getOrElse("--app-version", "1.0"),
      scalaVersion = mappings.getOrElse("--scala-version", "2.11.12"),
      sparkAvroVersion = mappings.getOrElse("--spark-avro", "4.0.0"),
      sparkCsvVersion =  mappings.getOrElse("--spark-csv", "1.5.0"),
      sparkVersion = mappings.getOrElse("--spark-version", "2.3.3")
    )
  }

  private def createArgumentsMap(args: Seq[String]): Map[String, String] = {
    Map(args.toList.sliding(2).toList map {
      case key :: value :: Nil if key.startsWith("--")  => key -> value
      case other => throw new IllegalArgumentException(s"Invalid argument specified near '${other.mkString(" ")}'")
    }: _*)
  }

}