package com.qwery.platform
package sparksql
package plugin

import java.io.File

import com.qwery.platform.sparksql.generator.{ApplicationSettings, SparkJobGenerator}
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
    applicationList foreach { app =>
      implicit val settings: ApplicationSettings = app.toSettings
      getLog.info(s"Generating '${settings.fullyQualifiedClassName}' (${settings.inputPath.getCanonicalPath})...")
      SparkJobGenerator.generate()
    }
  }

  private def applicationList: Seq[Application] = Option(applications).toSeq.flatMap(_.asScala)

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
      * @param app the [[Application]] to convert to an [[ApplicationSettings application settings]] instance
      */
    final implicit class ApplicationConversion(val app: Application) extends AnyVal {

      @inline def toSettings: ApplicationSettings = {
        import ApplicationSettings._

        import scala.collection.JavaConverters._

        // extract the class name and package from the fully qualified class name (e.g. "com.acme.CoyoteCrush")
        val fullyQualifiedClassName = Option(app.className).getOrElse(fail(app, "No class name specified"))
        val (className, packageName) = getClassAndPackageNames(fullyQualifiedClassName)

        // return the application settings
        new ApplicationSettings(
          appName = Option(app.appName).getOrElse(defaultAppName),
          appVersion = Option(app.appVersion).getOrElse(defaultAppVersion),
          className = className,
          isClassOnly = Option(app.classOnly).nonEmpty,
          defaultDB = Option(app.defaultDB).getOrElse(defaultDB),
          extendsClass = Option(app.extendsClass).getOrElse(defaultParentClass),
          inputPath = Option(app.inputPath).map(new File(_)).getOrElse(fail(app, "No input path specified")),
          outputPath = Option(app.outputPath).map(new File(_)).getOrElse(fail(app, "No output path specified")),
          packageName = packageName,
          properties = Option(app.properties.asScala.toMap).getOrElse(Map.empty),
          scalaVersion = Option(app.scalaVersion).getOrElse(defaultScalaVersion),
          sparkVersion = Option(app.sparkVersion).getOrElse(defaultSparkVersion),
          templateFile = Option(app.templateFile).map(new File(_))
        )
      }

      private def fail[A](app: Application, message: String): A =
        throw new IllegalArgumentException(s"$message in application '${app.appName}'")
    }

  }

}