package com.qwery.platform.sparksql

import java.io.File
import java.lang.reflect.{ParameterizedType, Type}

import com.qwery.util.OptionHelper._
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Spark Runtime Helper
  * @author lawrence.daniels@gmail.com
  */
object SparkRuntimeHelper {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Retrieves a recursive collection of files as rows of a data frame
    * @param path the given local file path
    * @return a [[DataFrame data frame]]
    */
  def getFiles(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    /**
      * Retrieves a recursive collection of files
      * @param file the root file or directory
      * @return a collection of [[File file]]s
      */
    def recurse(file: File): Stream[File] = {
      file match {
        case d if d.isDirectory => d.listFiles().toStream.flatMap(recurse)
        case f => Stream(f)
      }
    }

    (recurse(new File(path)) map { f =>
      (f.getName, f.getAbsolutePath, f.length(), f.canExecute, f.canRead, f.canWrite, f.getParent, f.isDirectory, f.isFile, f.isHidden)
    }) toDF("name", "absolutePath", "length", "canExecute", "canRead", "canWrite", "parent", "isDirectory", "isFile", "isHidden")
  }

  /**
    * Registers a UDF for use with Spark
    * @param name    the name of the UDF
    * @param `class` the fully qualified class name
    */
  def registerUDF(name: String, `class`: String)(implicit spark: SparkSession): Unit = {
    // instantiate the custom UDF instance
    val customUdf = Try(Class.forName(`class`).newInstance()) match {
      case Success(instance) => instance.asInstanceOf[Object]
      case Failure(e) => throw new IllegalStateException(s"Failed to instantiate UDF class '${`class`}': ${e.getMessage}", e)
    }

    // lookup the instance's UDF interface class
    val udfTrait = customUdf.getClass.getInterfaces
      .find(_.getName startsWith "org.apache.spark.sql.api.java.UDF")
      .orFail(s"Class '${`class`}' does not implement any of Spark's UDF interfaces")

    // lookup the instance's "Generics" return type
    // (e.g. "org.apache.spark.sql.api.java.UDF1<T1, R>" => R (java.lang.String)
    val returnType = customUdf.getClass.getGenericInterfaces
      .find(_.getTypeName.startsWith("org.apache.spark.sql.api.java.UDF"))
      .collect { case pt: ParameterizedType => pt }
      .flatMap(_.getActualTypeArguments.lastOption.map(_.toSpark))
      .orFail(s"Class '${`class`}' does not implement any of Spark's UDF interfaces")

    // register the UDF with Spark
    logger.info(s"Registering class '${`class`}' as UDF '$name'...")
    try {
      val registrar = spark.sqlContext.udf
      val method = registrar.getClass.getDeclaredMethod("register", classOf[String], udfTrait, classOf[DataType])
      method.invoke(registrar, name, customUdf, returnType)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to register class '${`class`}' as UDF '$name'", e)
    }
  }

  /**
    * Returns the Spark data type that corresponds to the given class name
    * @param className the given class name
    * @return the [[DataType]]
    */
  private def toSparkType(className: String): DataType = className match {
    case null | "null" => DataTypes.NullType
    case c if c.startsWith("[") => DataTypes.BinaryType // [B
    case "java.lang.Byte" | "scala.Byte" => DataTypes.ByteType
    case "java.util.Calendar" => DataTypes.CalendarIntervalType
    case "java.sql.Date" | "java.util.Date" => DataTypes.DateType
    case "java.lang.Double" | "scala.Double" => DataTypes.DoubleType
    case "java.lang.Float" | "scala.Float" => DataTypes.FloatType
    case "java.lang.Integer" | "scala.Int" => DataTypes.IntegerType
    case "java.lang.Long" | "scala.Long" => DataTypes.LongType
    case "java.lang.Short" | "scala.Short" => DataTypes.ShortType
    case "java.lang.String" => DataTypes.StringType
    case "java.sql.Timestamp" => DataTypes.TimestampType
    case _ =>
      logger.warn(s"Type '$className' was translated to 'DataTypes.StringType'")
      DataTypes.StringType
  }

  /**
    * JVM Class-To-Spark Conversion
    * @param `class` the given [[Class]]
    */
  final implicit class ClassToSparkConversion[T](val `class`: Class[T]) extends AnyVal {
    @inline def toSpark: DataType = toSparkType(`class`.getName)
  }

  /**
    * JVM Type-To-Spark Conversion
    * @param `type` the given [[Type]]
    */
  final implicit class TypeToSparkConversion(val `type`: Type) extends AnyVal {
    @inline def toSpark: DataType = toSparkType(`type`.getTypeName)
  }

  /**
    * DataFrame Enriched
    * @param dataFrame the given [[DataFrame]]
    */
  final implicit class DataFrameEnriched(val dataFrame: DataFrame) extends AnyVal {
    @inline def withGlobalTempView(name: String): DataFrame = {
      dataFrame.createOrReplaceGlobalTempView(name)
      dataFrame
    }
  }

}
