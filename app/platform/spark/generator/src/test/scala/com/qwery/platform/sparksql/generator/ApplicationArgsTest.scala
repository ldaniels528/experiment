package com.qwery.platform.sparksql.generator

import java.io.File

import org.scalatest.FunSpec

/**
  * Application Arguments Test
  * @author lawrence.daniels@gmail.com
  */
class ApplicationArgsTest extends FunSpec {

  describe(classOf[ApplicationArgs].getSimpleName) {

    it("should parse properly formatted arguments successfully") {
      val appArgs = ApplicationArgs(Array(
        "--app-name", "Hello World",
        "--app-version", "1.0.1",
        "--class-only", "N",
        "--default-db", "dev_test",
        "--scala-version", "2.11.11",
        "--spark-version", "2.3.2",
        "--template-file", "./src/main/scala/Test.scala"
      ))
      assert(appArgs.appName == "Hello World")
      assert(appArgs.appVersion == "1.0.1")
      assert(!appArgs.isClassOnly)
      assert(appArgs.defaultDB == "dev_test")
      assert(appArgs.scalaVersion == "2.11.11")
      assert(appArgs.sparkVersion == "2.3.2")
      assert(appArgs.templateFile.contains(new File("./src/main/scala/Test.scala")))
    }

    it("should failed to parse improperly formatted arguments") {
      assertThrows[IllegalArgumentException] {
        ApplicationArgs(Array(
          "--app-name", "Hello World",
          "--app-version", "1.0.1",
          "--class-only", "N",
          "--default-db", "dev_test",
          "--scala-version", "2.11.11",
          "--spark-version", "2.3.2",
          "--template-file", "./src/main/scala/Test.scala",
          "X", "Y"
        ))
      }
    }

  }

}
