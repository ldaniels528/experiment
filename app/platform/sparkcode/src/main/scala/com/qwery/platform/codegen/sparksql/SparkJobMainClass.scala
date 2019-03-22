package com.qwery
package platform.codegen.sparksql

import com.qwery.models.Invokable
import com.qwery.platform.codegen.sparksql.SparkCodeCompiler._

/**
  * Spark Job Main Class
  * @author lawrence.daniels@gmail.com
  */
case class SparkJobMainClass(className: String,
                             packageName: String,
                             invokable: Invokable,
                             imports: Seq[String]) {
  def generate: String = {
    s"""|package $packageName
        |
        |${imports.map(pkg => s"import $pkg").sortBy(s => s).mkString("\n")}
        |
        |class $className() extends Serializable {
        |  @transient
        |  private val logger = LoggerFactory.getLogger(getClass)
        |
        |  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
        |     import spark.implicits._
        |     ${invokable.compile}
        |  }
        |
        |}
        |
        |object $className {
        |   private[this] val logger = LoggerFactory.getLogger(getClass)
        |
        |   def main(args: Array[String]): Unit = {
        |     implicit val spark: SparkSession = createSparkSession("$className")
        |     new $className().start(args)
        |     spark.stop()
        |   }
        |
        |   def createSparkSession(appName: String): SparkSession = {
        |     val sparkConf = new SparkConf()
        |     val builder = SparkSession.builder()
        |       .appName(appName)
        |       .config(sparkConf)
        |       .enableHiveSupport()
        |
        |     // first attempt to create a clustered session
        |     try builder.getOrCreate() catch {
        |       // on failure, create a local one...
        |       case _: Throwable =>
        |         logger.warn(s"$$appName failed to connect to EMR cluster; starting local session...")
        |         builder.master("local[*]").getOrCreate()
        |     }
        |   }
        |}
        |""".stripMargin
  }
}