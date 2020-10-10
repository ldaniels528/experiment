package com.qwery.database.jdbc

/**
 * Qwery JDBC Parameters
 * @param initialCapacity the initial capacity of the collection
 * @param growthFactor    the amount the collection should expand by when the capacity is exceeded
 */
class JDBCParameters(initialCapacity: Int = 20, growthFactor: Int = 20) {
  private var parameters = new Array[Any](initialCapacity)
  private var parameterCount: Int = 0

  def apply(parameterNumber: Int): Any = parameters(toParameterIndex(parameterNumber))

  def clear(): Unit = parameterCount = 0

  def size: Int = parameterCount

  def toList: List[Any] = parameters.toList.slice(0, parameterCount)

  def update(parameterNumber: Int, value: Any): Unit = {
    val index = toParameterIndex(parameterNumber)
    parameterCount = parameterNumber max parameterCount

    // does the required size exceed the allocated size?
    if (parameterCount >= parameters.length) {
      val temp = parameters
      parameters = new Array[Any](parameterCount + growthFactor)
      System.arraycopy(temp, 0, parameters, 0, temp.length)
    }
    parameters(index) = value
  }

  @inline
  private def toParameterIndex(parameterNumber: Int): Int = {
    assert(parameterNumber > 0, s"Parameter numbers must be 1 or greater ($parameterNumber)")
    parameterNumber - 1
  }

}
