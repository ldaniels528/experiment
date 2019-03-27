package com.qwery
package platform.codegen.sparksql

import java.io.{File, PrintWriter}

import com.qwery.language.SQLLanguageParser
import com.qwery.models._
import com.qwery.platform.spark.die
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory
import com.qwery.util.StringHelper._

/**
  * Spark/Scala Job Generator
  * @author lawrence.daniels@gmail.com
  */
class SparkJobGenerator(className: String,
                        packageName: String,
                        outputPath: String,
                        appArgs: ApplicationArgs = ApplicationArgs(Nil)) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Generates the SBT build script
    * @return the [[File]] representing the generated build.sbt
    */
  def createBuildScript(): File = {
    import appArgs._
    writeToDisk(outputFile = new File(outputPath, "built.sbt")) {
      s"""|name := "$appName"
          |
          |version := "$appVersion"
          |
          |scalaVersion := "$scalaVersion"
          |
          |//test in assembly := {}
          |assemblyJarName in assembly := s"$${name.value}-$${version.value}.fat.jar"
          |assemblyMergeStrategy in assembly := {
          |  case PathList("META-INF", "services", _*) => MergeStrategy.filterDistinctLines
          |  case PathList("META-INF", "MANIFEST.MF", _*) => MergeStrategy.discard
          |  case PathList("META-INF", _*) => MergeStrategy.filterDistinctLines
          |  case PathList("org", "datanucleus", _*) => MergeStrategy.rename
          |  case PathList("com", "scoverage", _*) => MergeStrategy.discard
          |  case _ => MergeStrategy.first
          |}
          |
          |libraryDependencies ++= Seq(
          |   "com.databricks" %% "spark-avro" % "$sparkAvroVersion",
          |   "com.databricks" %% "spark-csv" % "$sparkCsvVersion",
          |   "org.apache.spark" %% "spark-core" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-hive" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-sql" % "$sparkVersion",
          |   //
          |   // placeholder dependencies
          |   "com.qwery" %% "core" % "${AppConstants.version}",
          |   "com.qwery" %% "platform-spark-codegen" % "${AppConstants.version}",
          |   "com.qwery" %% "language" % "${AppConstants.version}"
          |)
          |""".stripMargin
    }
  }

  /**
    * Generates an executable class file
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createMainClass(invokable: Invokable)(implicit settings: CompilerSettings): File = {
    // create the package directory
    val srcDir = new File(outputPath, "src/main/scala")
    val pkgDir = new File(srcDir, packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    writeToDisk(outputFile = new File(pkgDir, s"$className.scala")) {
      SparkJobMainClass(className, packageName, invokable, imports = List(
        "com.qwery.models._",
        StorageFormats.getObjectFullName,
        ResourceManager.getObjectFullName,
        "org.apache.spark.SparkConf",
        "org.apache.spark.sql.functions._",
        "org.apache.spark.sql.types.StructType",
        "org.apache.spark.sql.DataFrame",
        "org.apache.spark.sql.Row",
        "org.apache.spark.sql.SparkSession",
        "org.slf4j.LoggerFactory"
      )).generate(appArgs)
    }
  }

  /**
    * Generates a new SBT project
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createProject(invokable: Invokable)(implicit settings: CompilerSettings): File = {
    val startTime = System.currentTimeMillis()
    lazy val elapsedTime = System.currentTimeMillis() - startTime

    logger.info(s"[*] Generating Spark-Scala project '${appArgs.appName}' [${new File(outputPath).getCanonicalPath}]...")
    logger.info("[*] Building project structure...")
    createProjectStructure()

    logger.info("[*] Generating build script (build.sbt)...")
    createBuildScript()

    logger.info(s"[*] Generating class '$packageName.$className'...")
    val file = createMainClass(invokable)

    logger.info(s"[*] Process completed in $elapsedTime msec(s)")
    file
  }

  /**
    * Creates the SBT project structure
    * @return the [[File]] representing the `project` directory
    */
  def createProjectStructure(): File = {
    val projectDir = new File(outputPath, "project")
    if (!projectDir.mkdirs() && !projectDir.exists())
      die(s"Failed to create the SBT project directory ('${projectDir.getCanonicalPath}')")

    // generate the configuration files
    val configFiles = Seq(
      """addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")""" -> "assembly.sbt",
      """sbt.version=0.13.16""" -> "build.properties",
      """|addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.5")
         |addSbtPlugin("com.typesafe.akka" % "akka-sbt-plugin" % "2.2.3")""".stripMargin -> "plugins.sbt"
    )
    configFiles foreach { case (contents, path) =>
      writeToDisk(outputFile = new File(projectDir, path))(contents)
    }
    projectDir
  }

  /**
    * Writes the given contents to disk
    * @param outputFile the given [[File output file]]
    * @param contents   the given contents to write to disk
    * @return a reference to the [[File output file]]
    */
  private def writeToDisk(outputFile: File)(contents: => String): File = {
    new PrintWriter(outputFile).use(_.println(contents))
    outputFile
  }

}

/**
  * Spark Job Generator Companion
  * @author lawrence.daniels@gmail.com
  * @example sbt "project platform_sparkcode" "run ./samples/sql/adbook/adbook-client.sql ../adbook_poc/ com.coxautoinc.wtm.adbook.AdBookIngestSparkJob"
  */
object SparkJobGenerator {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = {
    import com.qwery.util.StringHelper._

    // get the input path, output path and fully qualified class name (e.g. "com.acme.CoyoteCrush") and flags
    val (inputPath, outputPath, (className, packageName), appArgs) = args.toList match {
      case input :: output :: fullClass :: flags => (input, output, getClassAndPackageNames(fullClass), ApplicationArgs(flags))
      case passedArgs =>
        logger.error(s"Invalid number of arguments passed: [${passedArgs.mkString(",")}]")
        die(s"java ${SparkJobGenerator.getObjectFullName} <inputPath> <outputPath> <outputClass> [<flags>]")
    }

    // create the compiler context
    implicit val settings: CompilerSettings = new CompilerSettings(
      inlineSQL = Some(!appArgs.sparkNative),
      defaultDB = Some(appArgs.defaultDB)
    )

      // generate the sbt project
    val model = SQLLanguageParser.parse(new File(inputPath))
    new SparkJobGenerator(className = className, packageName = packageName, outputPath = outputPath, appArgs = appArgs)
      .createProject(model)
  }

  /**
    * Extracts the class and package names from the given fully qualified class name
    * @param classNameWithPackage the given fully qualified class name (e.g. "com.acme.CoyoteFuture")
    * @return the class and package names (e.g. "com.acme.CoyoteFuture" => ["com.acme", "CoyoteFuture"])
    */
  private def getClassAndPackageNames(classNameWithPackage: String): (String, String) = {
    import com.qwery.util.StringHelper._
    classNameWithPackage.lastIndexOfOpt(".").map(classNameWithPackage.splitAt) match {
      case Some((packageName, className)) => (className.drop(1), packageName)
      case None => (classNameWithPackage, "com.examples")
    }
  }

}
