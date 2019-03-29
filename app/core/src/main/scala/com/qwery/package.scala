package com

/**
  * qwery package object
  * @author lawrence.daniels@gmail.com
  */
package object qwery {

  /**
    * Throws an exception
    * @param message the given exception message
    * @return [[Nothing]]
    */
  def die[A](message: String): A = throw new IllegalStateException(message)

}
