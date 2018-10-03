package com.qwery

/**
  * models package object
  * @author lawrence.daniels@gmail.com
  */
package object models {

  def die[A](message: String): A = throw new IllegalStateException(message)

}
