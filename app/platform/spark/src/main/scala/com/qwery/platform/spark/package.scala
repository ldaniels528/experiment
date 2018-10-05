package com.qwery.platform

import java.lang.reflect.Type

import org.apache.spark.sql.types.{DataType, DataTypes}

/**
  * spark package object
  * @author lawrence.daniels@gmail.com
  */
package object spark {

  def die[A](message: String, cause: Throwable = null): A = throw new IllegalStateException(message, cause)

  /**
    * JVM Class-To-Spark Conversion
    * @param `class` the given [[Class]]
    */
  final implicit class ClassToSparkConversion[T](val `class`: Class[T]) extends AnyVal {
    @inline def toSpark: DataType = `class`.getName match {
      case "scala.Byte" | "java.lang.Byte" => DataTypes.ByteType
      case "java.util.Date" => DataTypes.DateType
      case "java.lang.Double" => DataTypes.DoubleType
      case "scala.Int" | "java.lang.Integer" => DataTypes.IntegerType
      case "scala.Long" | "java.lang.Long" => DataTypes.LongType
      case "java.lang.Object" => DataTypes.StringType
      case "java.lang.String" => DataTypes.StringType
      case "java.sql.Timestamp" => DataTypes.TimestampType
      case unknown => die(s"Unsupported type conversion '$unknown'")
    }
  }

  /**
    * JVM Type-To-Spark Conversion
    * @param `type` the given [[Type]]
    */
  final implicit class TypeToSparkConversion[T](val `type`: Type) extends AnyVal {
    @inline def toSpark: DataType = `type`.getTypeName match {
      case "scala.Byte" | "java.lang.Byte" => DataTypes.ByteType
      case "java.util.Date" => DataTypes.DateType
      case "java.lang.Double" => DataTypes.DoubleType
      case "scala.Int" | "java.lang.Integer" => DataTypes.IntegerType
      case "scala.Long" | "java.lang.Long" => DataTypes.LongType
      case "java.lang.Object" => DataTypes.StringType
      case "java.lang.String" => DataTypes.StringType
      case "java.sql.Timestamp" => DataTypes.TimestampType
      case unknown => die(s"Unsupported type conversion '$unknown'")
    }
  }

}
