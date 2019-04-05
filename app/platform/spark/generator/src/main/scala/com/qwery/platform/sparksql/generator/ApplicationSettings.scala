package com.qwery.platform.sparksql.generator

import java.io.File

/**
  * Represents the Generated Spark Application's Settings
  * @param appName      the Spark application name (default: `"Untitled"`)
  * @param appVersion   the version identifier of your application (default: `"1.0"`)
  * @param className    the name of the class to generate (e.g. "CoyoteTrap")
  * @param isClassOnly  indicates whether only a Scala class should be generated (default: `false`)
  * @param defaultDB    the default database (default: `"global_temp"`)
  * @param extendsClass the optional class the generated class should extend (default: `Serializable`)
  * @param inputPath    the path of the input .sql file
  * @param outputPath   the path where the class files or SBT project will be generated
  * @param packageName  the package name of the class to be generated (e.g. "com.acme.coyote.tools")
  * @param properties   the optional Spark configuration properties
  * @param scalaVersion the Scala version the generated project will use (default: `"2.12.8"`)
  * @param sparkVersion the Apache Spark API version (default: `"2.4.1"`)
  * @param templateFile the optional template class to use in generating the Spark Job
  * @author lawrence.daniels@gmail.com
  */
case class ApplicationSettings(appName: String,
                               appVersion: String,
                               className: String,
                               isClassOnly: Boolean,
                               defaultDB: String,
                               extendsClass: String,
                               inputPath: File,
                               outputPath: File,
                               packageName: String,
                               properties: Map[String, String],
                               scalaVersion: String,
                               sparkVersion: String,
                               templateFile: Option[File]) {

  val fullyQualifiedClassName: String = s"$packageName.$className"

}

/**
  * Application Settings
  * @author lawrence.daniels@gmail.com
  */
object ApplicationSettings {
  val defaultAppName = "Untitled"
  val defaultAppVersion = "1.0"
  val defaultDatabase = "global_temp"
  val defaultParentClass = "Serializable"
  val defaultScalaVersion = "2.12.8"
  val defaultSparkVersion = "2.4.1"

  /**
    * Creates a new application Settings instance using the given command line arguments
    * @param args the given command line arguments
    * @return the [[ApplicationSettings]]
    */
  def apply(args: Seq[String] = Nil): ApplicationSettings = {
    // parse the command line arguments
    val mappings = createArgumentsMap(args)

    // extract the class name and package from the fully qualified class name (e.g. "com.acme.CoyoteCrush")
    val fullyQualifiedClassName = mappings.require("1.0--class-name")
    val (className, packageName) = getClassAndPackageNames(fullyQualifiedClassName)

    // create the application settings
    ApplicationSettings(
      appName = mappings.getOrElse("--app-name", defaultAppName),
      appVersion = mappings.getOrElse("--app-version", defaultAppVersion),
      className = className,
      extendsClass = mappings.getOrElse("--extends-class", defaultParentClass),
      isClassOnly = mappings.isTrue("--class-only"),
      defaultDB = mappings.getOrElse("--default-db", defaultDatabase),
      inputPath = new File(mappings.require("--input-path")),
      outputPath = new File(mappings.require("--output-path")),
      packageName = packageName,
      properties = mappings,
      scalaVersion = mappings.getOrElse("--scala-version", defaultScalaVersion),
      sparkVersion = mappings.getOrElse("--spark-version", defaultSparkVersion),
      templateFile = mappings.get("--template-file").map(new File(_))
    )
  }

  /**
    * Creates a new [[ApplicationSettings]] instance
    * @param appName      the Spark application name (default: `"Untitled"`)
    * @param appVersion   the version identifier of your application (default: `"1.0"`)
    * @param className    the name of the class to generate (e.g. "CoyoteTrap")
    * @param isClassOnly  indicates whether only a Scala class should be generated (default: `false`)
    * @param defaultDB    the default database (default: `"global_temp"`)
    * @param extendsClass the optional class the generated class should extend (default: `Serializable`)
    * @param inputPath    the path of the input .sql file
    * @param outputPath   the path where the class files or SBT project will be generated
    * @param packageName  the package name of the class to be generated (e.g. "com.acme.coyote.tools")
    * @param properties   the optional Spark configuration properties
    * @param scalaVersion the Scala version the generated project will use (default: `"2.12.8"`)
    * @param sparkVersion the Apache Spark API version (default: `"2.4.1"`)
    * @param templateFile the optional template class to use in generating the Spark Job
    * @return the [[ApplicationSettings]] instance
    */
  def apply(appName: Option[String],
            appVersion: Option[String],
            className: Option[String],
            isClassOnly: Option[Boolean],
            defaultDB: Option[String],
            extendsClass: Option[String],
            inputPath: Option[File],
            outputPath: Option[File],
            packageName: Option[String],
            properties: Option[Map[String, String]],
            scalaVersion: Option[String],
            sparkVersion: Option[String],
            templateFile: Option[File]): ApplicationSettings = {
    new ApplicationSettings(
      appName = appName.getOrElse(defaultAppName),
      appVersion = appVersion.getOrElse(defaultAppVersion),
      className = className.getOrElse(fail("className")),
      extendsClass = extendsClass.getOrElse(defaultParentClass),
      isClassOnly = isClassOnly.contains(true),
      defaultDB = defaultDB.getOrElse(defaultDatabase),
      inputPath = inputPath.getOrElse(fail("inputPath")),
      outputPath = outputPath.getOrElse(fail("outputPath")),
      packageName = packageName.getOrElse(fail("packageName")),
      properties = properties.getOrElse(Map.empty),
      scalaVersion = scalaVersion.getOrElse(defaultScalaVersion),
      sparkVersion = sparkVersion.getOrElse(defaultSparkVersion),
      templateFile = templateFile
    )
  }

  /**
    * Creates a key-value mapping from the command line arguments
    * @param args the given command line arguments
    * @return a [[Map map]] containing key-value pairs representing the command line arguments
    */
  private def createArgumentsMap(args: Seq[String]): Map[String, String] = {
    Map(args.toList.sliding(2, 2).toList map {
      case key :: value :: Nil if key.startsWith("--") => key -> value
      case other => throw new IllegalArgumentException(s"Invalid argument specified near '${other.mkString(" ")}'")
    }: _*)
  }

  /**
    * Extracts the class and package names from the given fully qualified class name
    * @param fullyQualifiedClassName the given fully qualified class name (e.g. "com.acme.CoyoteFuture")
    * @return the class and package names (e.g. "com.acme.CoyoteFuture" => ["com.acme", "CoyoteFuture"])
    */
  def getClassAndPackageNames(fullyQualifiedClassName: String): (String, String) = {
    import com.qwery.util.StringHelper._
    fullyQualifiedClassName.lastIndexOfOpt(".").map(fullyQualifiedClassName.splitAt) match {
      case Some((packageName, className)) => (className.drop(1), packageName)
      case None => (fullyQualifiedClassName, "com.examples.spark")
    }
  }

  def fail[A](property: String): A =
    throw new IllegalArgumentException(s"Required property '$property' is missing")

  /**
    * Map Extensions
    * @param mappings the given [[Map mappings]]
    */
  final implicit class MapExtensions(val mappings: Map[String, String]) extends AnyVal {

    @inline def isTrue(name: String): Boolean = mappings.get(name).exists(v => Seq("t", "true", "y", "yes").contains(v.toLowerCase))

    @inline def require(name: String): String = mappings.getOrElse(name, fail(name))

  }

}