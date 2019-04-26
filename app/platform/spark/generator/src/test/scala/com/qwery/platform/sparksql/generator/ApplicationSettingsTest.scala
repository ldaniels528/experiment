package com.qwery.platform.sparksql.generator

import java.io.File

import org.scalatest.FunSpec

/**
  * Application Settings Test
  * @author lawrence.daniels@gmail.com
  */
class ApplicationSettingsTest extends FunSpec {

  describe(classOf[ApplicationSettings].getSimpleName) {

    it("should parse properly formatted arguments successfully") {
      val settings = ApplicationSettings.fromArgs(
        "--input-path", "./scripts/daily-report.sql",
        "--output-path", "./src/main/scala/",
        "--class-name", "com.acme.spark.RoadRunnerDetector",
        "--app-name", "Hello World",
        "--app-version", "1.0.1",
        "--class-only", "N",
        "--default-db", "dev_test",
        "--scala-version", "2.11.11",
        "--spark-version", "2.3.2",
        "--template-file", "./src/main/resources/Template.scala.txt"
      )
      assert(settings.appName == "Hello World")
      assert(settings.appVersion == "1.0.1")
      assert(!settings.isClassOnly)
      assert(settings.className == "RoadRunnerDetector")
      assert(settings.defaultDB == "dev_test")
      assert(settings.inputPath == new File("./scripts/daily-report.sql"))
      assert(settings.outputPath == new File("./src/main/scala/"))
      assert(settings.packageName == "com.acme.spark")
      assert(settings.scalaVersion == "2.11.11")
      assert(settings.sparkVersion == "2.3.2")
      assert(settings.templateFile.contains(new File("./src/main/resources/Template.scala.txt")))
    }

  }

}
