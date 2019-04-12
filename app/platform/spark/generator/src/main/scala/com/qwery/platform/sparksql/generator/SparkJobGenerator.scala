package com.qwery
package platform
package sparksql.generator

import java.io.{File, PrintWriter}

import com.qwery.language.SQLLanguageParser
import com.qwery.models._
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

/**
  * Spark/Scala Job Generator
  * @author lawrence.daniels@gmail.com
  */
class SparkJobGenerator() {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Generates an SBT project for the given [[Invokable]]
    * @param invokable the given [[Invokable]]
    * @param settings the implicit [[ApplicationSettings]]
    * @param ctx the implicit [[CompileContext]]
    * @return a [[File]] representing the project or class
    */
  def generate(invokable: Invokable)(implicit settings: ApplicationSettings, ctx: CompileContext): File = {
    // add global imports
    ctx.addImports(
      "org.apache.spark.SparkConf",
      "org.apache.spark.sql.DataFrame",
      "org.apache.spark.sql.SaveMode",
      "org.apache.spark.sql.SparkSession",
      "org.slf4j.LoggerFactory",
      "com.qwery.platform.sparksql.SparkRuntimeHelper._"
    )

    if (!settings.isClassOnly) createProject(invokable) else {
      settings.templateFile match {
        case Some(templateFile) => createSparkJob(loadTemplate(templateFile), invokable)
        case None => createSparkJob(defaultTemplate, invokable)
      }
    }
  }

  /**
    * Generates the SBT build script
    * @return the [[File]] representing the generated build.sbt
    */
  private def createBuildScript()(implicit settings: ApplicationSettings): File = {
    import settings._
    writeToDisk(outputFile = new File(outputPath, "built.sbt")) {
      s"""|name := "$appName"
          |
          |version := "$appVersion"
          |
          |scalaVersion := "$scalaVersion"
          |
          |test in assembly := {}
          |mainClass in assembly := Some("$packageName.$className")
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
          |   "org.apache.spark" %% "spark-avro" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-core" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-hive" % "$sparkVersion",
          |   "org.apache.spark" %% "spark-sql" % "$sparkVersion",
          |   "com.qwery" %% "spark-tools" % "0.4.0"
          |)
          |""".stripMargin('|')
    }
  }

  /**
    * Generates a new SBT project
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  private def createProject(invokable: Invokable)(implicit settings: ApplicationSettings, ctx: CompileContext): File = {
    val startTime = System.currentTimeMillis()
    lazy val elapsedTime = System.currentTimeMillis() - startTime

    logger.info(s"[*] Generating Spark-Scala project '${settings.appName}' [${settings.outputPath.getCanonicalPath}]...")
    logger.info("[*] Building project structure...")
    createProjectStructure()

    logger.info("[*] Generating build script (build.sbt)...")
    createBuildScript()

    // generate the class file
    val file = settings.templateFile
      .map(loadTemplate)
      .map(createSparkJob(_, invokable))
      .getOrElse(createSparkJob(defaultTemplate, invokable))

    logger.info(s"[*] Process completed in $elapsedTime msec(s)")
    file
  }

  /**
    * Creates the SBT project structure
    * @return the [[File]] representing the `project` directory
    */
  private def createProjectStructure()(implicit settings: ApplicationSettings): File = {
    val projectDir = new File(settings.outputPath, "project")
    if (!projectDir.mkdirs() && !projectDir.exists())
      die(s"Failed to create the SBT project directory ('${projectDir.getCanonicalPath}')")

    // generate the configuration files
    val configFiles = Seq(
      """addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")""" -> "assembly.sbt",
      "sbt.version=1.2.8" -> "build.properties",
      "" -> "plugins.sbt"
    )
    configFiles foreach { case (contents, path) =>
      writeToDisk(outputFile = new File(projectDir, path))(contents)
    }
    projectDir
  }

  /**
    * Generates an executable class file
    * @param invokable the [[Invokable]] for which to generate code
    * @return the [[File]] representing the generated Main class
    */
  private def createSparkJob(templateString: String, invokable: Invokable)
                    (implicit settings: ApplicationSettings, ctx: CompileContext): File = {
    import settings._

    // create the package directory
    val pkgDir = new File(inferSourceDir(outputPath), packageName.replaceAllLiterally(".", File.separator))
    if (!pkgDir.mkdirs() && !pkgDir.exists()) die(s"Failed to create the package directory (package '$packageName')")

    // write the class to disk
    writeToDisk(outputFile = new File(pkgDir, s"$className.scala")) {
      CodeTemplate.fromString(templateString).generate(invokable)
    }
  }

  /**
    * Returns the default Spark job template
    */
  private def defaultTemplate: String = {
    s"""|package {{ packageName }}
        |
        |{{ imports }}
        |
        |/**
        |  * {{ appName }} Spark Job
        |  * Generated on {{ date:MM-dd-yyyy }} at {{ date:hh:mma }}
        |  * @author {{ jvm:user.name }}
        |  */
        |class {{ className }}() extends {{ extendsClass }} {
        |  @transient
        |  private lazy val logger = LoggerFactory.getLogger(getClass)
        |
        |  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
        |     import spark.implicits._
        |
        |     {{ flow }}
        |  }
        |
        |}
        |
        |object {{ className }} {
        |   private[this] val logger = LoggerFactory.getLogger(getClass)
        |
        |   def main(args: Array[String]): Unit = {
        |     implicit val spark: SparkSession = createSparkSession()
        |     new {{ className }}().start(args)
        |     spark.stop()
        |   }
        |
        |   def createSparkSession(): SparkSession = {
        |     val sparkConf = new SparkConf()
        |     val builder = SparkSession.builder()
        |       .appName("{{ appName }}")
        |       .config(sparkConf)
        |       .enableHiveSupport()
        |
        |     // first attempt to create a clustered session
        |     try builder.getOrCreate() catch {
        |       // on failure, create a local one...
        |       case _: Throwable =>
        |         System.setSecurityManager(null)
        |         logger.warn("Application '{{ appName }}' failed to connect to EMR cluster; starting local session...")
        |         builder.master("local[*]").getOrCreate()
        |     }
        |   }
        |}
        |""".stripMargin('|')
  }

  /**
    * Determines the source directory for the desired project or class file
    * @param outputFile the given output [[File file]]
    * @param settings the implicit [[ApplicationSettings]]
    * @return the source [[File directory]]
    */
  private def inferSourceDir(outputFile: File)(implicit settings: ApplicationSettings): File = {
    if (settings.isClassOnly) outputFile else new File(outputFile, "src/main/scala")
  }

  /**
    * Retrieves the template content from a file
    * @param theTemplateFile the given template [[File file]]
    * @return the template content
    */
  private def loadTemplate(theTemplateFile: File)(implicit settings: ApplicationSettings): String = {
    import settings._
    logger.info(s"[*] Generating class '$packageName.$className' from template '${theTemplateFile.getCanonicalPath}'...")
    scala.io.Source.fromFile(theTemplateFile).use(_.getLines().mkString("\n"))
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

  /**
    * For stand alone operation
    * @param args the given command line arguments
    */
  def main(args: Array[String]): Unit = generate()(ApplicationSettings.fromArgs(args))

  /**
    * Performs the Spark job generation
    * @param settings the implicit [[ApplicationSettings]]
    */
  def generate()(implicit settings: ApplicationSettings): Unit = {
    import settings._

    // generate the sbt project
    val model = SQLLanguageParser.parse(inputPath)
    val ctx = CompileContext(model)
    new SparkJobGenerator().generate(model)(settings, ctx)
  }

}
