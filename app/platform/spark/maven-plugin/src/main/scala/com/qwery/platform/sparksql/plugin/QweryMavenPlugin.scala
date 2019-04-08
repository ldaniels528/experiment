package com.qwery.platform
package sparksql
package plugin

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

        // extract the class name and package from the fully qualified class name (e.g. "com.acme.CoyoteCrush")
        val fullyQualifiedClassName = Option(app.className).getOrElse(required(app, "No class name specified"))
        val (className, packageName) = getClassAndPackageNames(fullyQualifiedClassName)

        // return the application settings
        new ApplicationSettings.Builder()
          .withAppName(app.appName)
          .withAppVersion(app.appVersion)
          .withClassName(className)
          .withClassOnly(app.classOnly)
          .withDefaultDB(app.defaultDB)
          .withExtendsClass(app.extendsClass)
          .withInputPath(app.inputPath)
          .withOutputPath(app.outputPath)
          .withPackageName(packageName)
          .withProperties(app.properties)
          .withScalaVersion(app.scalaVersion)
          .withSparkVersion(app.sparkVersion)
          .withTemplateFile(app.templateFile)
          .build
      }

      private def required[A](app: Application, message: String): A =
        throw new IllegalArgumentException(s"$message in application '${app.appName}'")
    }

  }

}