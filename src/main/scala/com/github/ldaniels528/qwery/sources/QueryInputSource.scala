package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ResultSet
import com.github.ldaniels528.qwery.ops.Executable

/**
  * Query Input Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryInputSource extends QuerySource {

  /**
    * Executes a query
    * @param query the given query
    * @return the result set
    */
  def execute(query: Executable): ResultSet

}
