package com.qwery
package platform
package sparksql.generator

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import com.qwery.language.SQLLanguageParser
import com.qwery.models._
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Properties

/**
  * Spark/Scala Job Generator
  * @author lawrence.daniels@gmail.com
  */
class SparkJobGenerator(className: String,
                        packageName: String,
                        outputPath: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Generates the SBT build script
    * @return the [[File]] representing the generated build.sbt
    */
  def createBuildScript()(implicit appArgs: ApplicationArgs): File = {
    import appArgs._
    writeToDisk(outputFile = new File(outputPath, "built.sbt")) {
      s"""~name := "$appName"
         ~
          ~version := "$appVersion"
         ~
          ~scalaVersion := "$scalaVersion"
         ~
          ~test in assembly := {}
         ~mainClass in assembly := Some("$packageName.$className")
         ~assemblyJarName in assembly := s"$${name.value}-$${version.value}.fat.jar"
         ~assemblyMergeStrategy in assembly := {
         ~  case PathList("META-INF", "services", _*) => MergeStrategy.filterDistinctLines
         ~  case PathList("META-INF", "MANIFEST.MF", _*) => MergeStrategy.discard
         ~  case PathList("META-INF", _*) => MergeStrategy.filterDistinctLines
         ~  case PathList("org", "datanucleus", _*) => MergeStrategy.rename
         ~  case PathList("com", "scoverage", _*) => MergeStrategy.discard
         ~  case _ => MergeStrategy.first
         ~}
         ~
          ~libraryDependencies ++= Seq(
         ~   "com.databricks" %% "spark-avro" % "$sparkAvroVersion",
         ~   "com.databricks" %% "spark-csv" % "$sparkCsvVersion",
         ~   "org.apache.spark" %% "spark-core" % "$sparkVersion",
         ~   "org.apache.spark" %% "spark-hive" % "$sparkVersion",
         ~   "org.apache.spark" %% "spark-sql" % "$sparkVersion",
         ~   //
         ~   // Qwery dependencies
         ~   "com.qwery" %% "core" % "${AppConstants.version}",
         ~   "com.qwery" %% "platform-spark-generator" % "${AppConstants.version}",
         ~   "com.qwery" %% "language" % "${AppConstants.version}"
         ~)
         ~""".stripMargin('~')
    }
  }

  /**
    * Generates an executable class file
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createMainClass(invokable: Invokable)(implicit appArgs: ApplicationArgs, ctx: CompileContext): File = {
    // create the package directory
    val pkgDir = new File(inferSourceDir(outputPath), packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    logger.info(s"[*] Generating class '$packageName.$className'...")
    writeToDisk(outputFile = new File(pkgDir, s"$className.scala")) {
      generateClassWithMain(invokable, imports = List(
        "org.apache.spark.SparkConf",
        "org.apache.spark.sql.DataFrame",
        "org.apache.spark.sql.SaveMode",
        "org.apache.spark.sql.SparkSession",
        "org.slf4j.LoggerFactory"
      ))
    }
  }

  /**
    * Generates an executable class file
    * @param templateFile the given template [[File file]]
    * @param invokable    the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createMainClassFromTemplate(templateFile: File, invokable: Invokable)
                                 (implicit appArgs: ApplicationArgs, ctx: CompileContext): File = {
    // create the package directory
    val pkgDir = new File(inferSourceDir(outputPath), packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    logger.info(s"[*] Generating class '$packageName.$className' from template '${templateFile.getCanonicalPath}'...")
    writeToDisk(outputFile = new File(pkgDir, s"$className.scala")) {
      generateClassFromTemplate(templateFile, invokable, imports = List(
        "org.apache.spark.sql.functions._",
        "org.slf4j.LoggerFactory"
      ))
    }
  }

  /**
    * Generates a new SBT project
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  def createProject(invokable: Invokable)(implicit appArgs: ApplicationArgs, ctx: CompileContext): File = {
    val startTime = System.currentTimeMillis()
    lazy val elapsedTime = System.currentTimeMillis() - startTime

    logger.info(s"[*] Generating Spark-Scala project '${appArgs.appName}' [${new File(outputPath).getCanonicalPath}]...")
    logger.info("[*] Building project structure...")
    createProjectStructure()

    logger.info("[*] Generating build script (build.sbt)...")
    createBuildScript()

    // generate the class file
    val file = appArgs.templateClass.map(createMainClassFromTemplate(_, invokable)).getOrElse(createMainClass(invokable))

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
      """sbt.version=0.13.18""" -> "build.properties",
      """|addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.5")
         |addSbtPlugin("com.typesafe.akka" % "akka-sbt-plugin" % "2.2.3")""".stripMargin -> "plugins.sbt"
    )
    configFiles foreach { case (contents, path) =>
      writeToDisk(outputFile = new File(projectDir, path))(contents)
    }
    projectDir
  }

  def generate(invokable: Invokable)(implicit appArgs: ApplicationArgs, ctx: CompileContext): File = {
    if (!appArgs.isClassOnly) createProject(invokable) else {
      appArgs.templateClass match {
        case Some(templateFile) => createMainClassFromTemplate(templateFile, invokable)
        case None => createMainClass(invokable)
      }
    }
  }

  /**
    * Generates a class from a template file
    * @param templateFile the given template [[File file]]
    * @param invokable    the [[Invokable]] for which to generate code
    * @param imports      the collection of imports
    * @param appArgs      the implicit [[ApplicationArgs]]
    * @return an option of the contents f the generated file
    */
  private def generateClassFromTemplate(templateFile: File, invokable: Invokable, imports: Seq[String])
                                       (implicit appArgs: ApplicationArgs, ctx: CompileContext): String = {
    import SparkCodeCompiler.Implicits._
    import com.qwery.util.StringHelper._

    // read the contents of the template file
    val code = new StringBuilder(Source.fromFile(templateFile).use(_.getLines().mkString("\n")))
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
        val replacement = property.toLowerCase match {
          case s if s.startsWith("env:") => Properties.envOrElse(s.drop(4), "")
          case s if s.startsWith("jvm:") => System.getProperty(s.drop(4), "")
          case s if s.startsWith("prop:") => appArgs.properties.getOrElse(s.drop(5), "")
          case "appname" => appArgs.appName
          case "appversion" => appArgs.appVersion
          case "classname" => className
          case "date" => new SimpleDateFormat("MM-dd-yyyy").format(new java.util.Date())
          case "datetime" => new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(new java.util.Date())
          case "invoke" => invokable.compile
          case "packagename" => packageName
          case "templatefile" => appArgs.templateClass.map(_.getCanonicalPath).getOrElse("")
          case "time" => new SimpleDateFormat("HH:mm:ss").format(new java.util.Date())
          case _ => die(s"Qwery property '$property' is invalid")
        }

        // replace the tag
        code.replace(start, end, replacement)
        lastIndex = start + replacement.length
      }
      done = results.isEmpty
    }
    code.toString()
  }

  /**
    * Generates a class file with a main() method
    * @param invokable the [[Invokable]] for which to generate code
    * @param imports   the collection of imports
    * @param appArgs   the implicit [[ApplicationArgs]]
    * @return the contents f the generated file
    */
  private def generateClassWithMain(invokable: Invokable, imports: Seq[String])
                                   (implicit appArgs: ApplicationArgs, ctx: CompileContext): String = {
    import SparkCodeCompiler.Implicits._
    import appArgs._
    s"""~package $packageName
       ~
        ~${imports.map(pkg => s"import $pkg").sortBy(s => s).mkString("\n")}
       ~
        ~class $className() extends $extendsClass {
       ~  @transient
       ~  private lazy val logger = LoggerFactory.getLogger(getClass)
       ~
        ~  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
       ~     import spark.implicits._
       ~     ${invokable.compile}
       ~  }
       ~
        ~}
       ~
        ~object $className {
       ~   private[this] val logger = LoggerFactory.getLogger(getClass)
       ~
        ~   def main(args: Array[String]): Unit = {
       ~     implicit val spark: SparkSession = createSparkSession()
       ~     new $className().start(args)
       ~     spark.stop()
       ~   }
       ~
        ~   def createSparkSession(): SparkSession = {
       ~     val sparkConf = new SparkConf()
       ~     val builder = SparkSession.builder()
       ~       .appName("$appName")
       ~       .config(sparkConf)
       ~       .enableHiveSupport()
       ~
        ~     // first attempt to create a clustered session
       ~     try builder.getOrCreate() catch {
       ~       // on failure, create a local one...
       ~       case _: Throwable =>
       ~         System.setSecurityManager(null)
       ~         logger.warn("Application '$appName' failed to connect to EMR cluster; starting local session...")
       ~         builder.master("local[*]").getOrCreate()
       ~     }
       ~   }
       ~}
       ~""".stripMargin('~')
  }

  private def inferSourceDir(outputPath: String)(implicit appArgs: ApplicationArgs): File = {
    if (appArgs.isClassOnly) new File(outputPath) else new File(outputPath, "src/main/scala")
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
    val (inputPath, outputPath, classNameWithPackage, appArgs) = args.toList match {
      case input :: output :: fullClass :: flags => (input, output, fullClass, ApplicationArgs(flags))
      case passedArgs =>
        logger.error(s"Invalid number of arguments passed: [${passedArgs.mkString(",")}]")
        die(s"java ${SparkJobGenerator.getObjectFullName} <inputPath> <outputPath> <outputClass> [<flags>]")
    }

    // generate the application
    generate(inputPath, outputPath, classNameWithPackage)(appArgs)
  }

  /**
    * Performs the Spark job generation
    * @param inputPath            the input path of the SQL file to translate
    * @param outputPath           the output path where the class file or project will be generated
    * @param classNameWithPackage the fully qualified class name (e.g. "com.acme.traps.CoyoteTrap")
    * @param appArgs              the implicit [[ApplicationArgs]]
    */
  def generate(inputPath: String, outputPath: String, classNameWithPackage: String)(implicit appArgs: ApplicationArgs): Unit = {
    // extract the class name and package from the fully qualified class name (e.g. "com.acme.CoyoteCrush") and flags
    val (className, packageName) = getClassAndPackageNames(classNameWithPackage)

    // generate the sbt project
    val model = SQLLanguageParser.parse(new File(inputPath))
    val ctx = CompileContext(model)
    new SparkJobGenerator(className = className, packageName = packageName, outputPath = outputPath)
      .generate(model)(appArgs, ctx)
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
      case None => (classNameWithPackage, "com.examples.spark")
    }
  }

}
