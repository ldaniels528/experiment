package com.qwery.platform.spark

import com.qwery.models.Column
import org.apache.spark.sql.DataFrame

/**
  * Spark Procedure
  * @param name   the name of the procedure
  * @param params the procedure's parameters
  * @param code   the procedure's executable code
  */
case class SparkProcedure(name: String, params: Seq[Column], code: SparkInvokable) {

  /**
    * Invokes the procedure
    * @param args the procedure-call arguments
    * @param rc   the implicit [[SparkQweryContext]]
    * @return the option of a [[DataFrame]]
    */
  def invoke(args: List[Any])(implicit rc: SparkQweryContext): Option[DataFrame] = {
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