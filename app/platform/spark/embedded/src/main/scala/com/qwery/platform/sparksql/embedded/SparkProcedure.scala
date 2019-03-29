package com.qwery.platform.sparksql.embedded

import com.qwery.models.Column
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Spark Procedure
  * @param name   the name of the procedure
  * @param params the procedure's parameters
  * @param code   the procedure's executable code
  */
case class SparkProcedure(name: String, params: Seq[Column], code: SparkInvokable) extends SparkInvokable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    logger.info(s"Registering Procedure '$name' with parameters ${params.map(p => s"${p.name}:${p.`type`}").mkString(", ")}...")
    rc += this
    None
  }

  /**
    * Invokes the procedure
    * @param args the procedure-call arguments
    * @param rc   the implicit [[EmbeddedSparkContext]]
    * @return the option of a [[DataFrame]]
    */
  def invoke(args: List[Any])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._

    // create a new scope
    rc.withScope { scope =>
      // populate the scope with the input arguments
      params zip args foreach { case (param, value) => scope(param.name) = value }

      // also create a data set containing the same data
      code.execute(input = rc.createDataSet(columns = params, data = List(args)))
    }
  }

}
