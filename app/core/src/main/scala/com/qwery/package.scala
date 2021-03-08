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

  object implicits {

    final implicit class MagicImplicits[A](val value: A) extends AnyVal {
      @inline def as[B](f: A => B): B = f(value)
    }

  }

}
