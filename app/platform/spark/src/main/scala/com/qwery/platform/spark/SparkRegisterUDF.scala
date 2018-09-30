package com.qwery.platform.spark

import java.lang.reflect.ParameterizedType

import com.qwery.models.UserDefinedFunction
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Registers a UDF for use with Spark
  * @param function the [[UserDefinedFunction]]
  */
case class SparkRegisterUDF(function: UserDefinedFunction) extends SparkInvokable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    // instantiate the custom UDF instance
    val customUdf = Try(Class.forName(function.`class`).newInstance()) match {
      case Success(instance) => instance.asInstanceOf[Object]
      case Failure(e) => die(s"Failed to instantiate UDF class '${function.`class`}': ${e.getMessage}", e)
    }

    // lookup the instance's UDF interface class
    val udfTrait = customUdf.getClass.getInterfaces
      .find(_.getName startsWith "org.apache.spark.sql.api.java.UDF")
      .orFail(s"Class '${function.`class`}' does not implement any of Spark's UDF interfaces")

    // lookup the instance's "Generics" return type
    // (e.g. "org.apache.spark.sql.api.java.UDF1<T1, R>" => R (java.lang.String)
    val returnType = customUdf.getClass.getGenericInterfaces
      .find(_.getTypeName.startsWith("org.apache.spark.sql.api.java.UDF"))
      .collect { case pt: ParameterizedType => pt }
      .flatMap(_.getActualTypeArguments.lastOption.map(_.toSpark))
      .orFail(s"Class '${function.`class`}' does not implement any of Spark's UDF interfaces")

    // register the UDF with Spark
    logger.info(s"Registering class '${function.`class`}' as UDF '${function.name}'...")
    try {
      val registrar = rc.spark.sqlContext.udf
      val method = registrar.getClass.getDeclaredMethod("register", classOf[String], udfTrait, classOf[DataType])
      method.invoke(registrar, function.name, customUdf, returnType)
      None
    } catch {
      case e: Exception =>
        die(s"Failed to register class '${function.`class`}' as UDF '${function.name}'", e)
    }
  }

}
