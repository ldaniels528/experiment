package com.qwery.models

import com.qwery.models.expressions.VariableRef

/**
  * Main Program - application entry point
  * {{{
  * MAIN PROGRAM 'StockIngest'
  *   WITH ARGUMENTS AS @args
  *   WITH ENVIRONMENT AS @env
  *   WITH STREAM PROCESSING
  * AS
  * BEGIN
  *   INSERT OVERWRITE LOCATION './data/companies/service' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
  *   SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
  *   FROM Companies WHERE Industry = 'EDP Services'
  * END
  * }}}
  * @param name      the name of the application/job
  * @param code      the code to execute
  * @param streaming indicates whether the job is streaming (or conversely batch)
  * @author lawrence.daniels@gmail.com
  */
case class MainProgram(name: String,
                       code: Invokable,
                       arguments: Option[VariableRef] = None,
                       environment: Option[VariableRef] = None,
                       hiveSupport: Boolean = false,
                       streaming: Boolean = false) extends Invokable