package com.qwery.platform.sparksql.plugin

import java.io.File

import com.qwery.platform.sparksql.generator.{ApplicationArgs, SparkJobGenerator}
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations.{LifecyclePhase, Mojo, Parameter}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  * Qwery Maven Plugin
  * @author lawrence.daniels@gmail.com
  */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.COMPILE)
class QweryMavenPlugin extends AbstractMojo {
  @Parameter(property = "applications")
  @BeanProperty var applications: java.util.List[Application] = _

  override def execute(): Unit = {
    import QweryMavenPlugin.Implicits._
    getLog.info(s"Processing ${applicationList.size} applications...")
    for {
      app <- applicationList
      appArgs = app.toApplicationArgs
      inputPath = Option(app.inputPath).getOrElse(fail(app, "No input path specified"))
      outputPath = Option(app.outputPath).getOrElse(fail(app, "No output path specified"))
      classNameWithPackage = Option(app.className).getOrElse(fail(app, "No class name specified"))
    } {
      getLog.info(s"Generating '$classNameWithPackage' (${new File(inputPath).getCanonicalPath})...")
      SparkJobGenerator.generate(inputPath, outputPath, classNameWithPackage)(appArgs)
    }
  }

  private def applicationList: Seq[Application] = Option(applications).toSeq.flatMap(_.asScala)

  private def fail[A](app: Application, message: String): A =
    throw new IllegalArgumentException(s"$message in application '${app.appName}'")

}

/**
  * Qwery Maven Plugin Companion
  * @author lawrence.daniels@gmail.com
  */
object QweryMavenPlugin {

  /**
    * Implicit conversions
    */
  object Implicits {

    /**
      * Application Conversion
      * @param app the [[Application]] to convert to an [[ApplicationArgs]] instance
      */
    final implicit class ApplicationConversion(val app: Application) extends AnyVal {

      @inline def toApplicationArgs: ApplicationArgs = {
        new ApplicationArgs(
          appName = Option(app.appName).getOrElse("Untitled"),
          appVersion = Option(app.appVersion).getOrElse("1.0"),
          isClassOnly = Option(app.classOnly).nonEmpty,
          defaultDB = Option(app.defaultDB).getOrElse("global_temp"),
          extendsClass = Option(app.extendsClass).getOrElse("Serializable"),
          scalaVersion = Option(app.scalaVersion).getOrElse("2.11.12"),
          sparkAvroVersion = Option(app.sparkAvroVersion).getOrElse("4.0.0"),
          sparkCsvVersion = Option(app.sparkCsvVersion).getOrElse("1.5.0"),
          sparkVersion = Option(app.sparkVersion).getOrElse("2.3.3"),
          templateClass = Option(app.templateClass).map(new File(_))
        )
      }
    }

  }

}