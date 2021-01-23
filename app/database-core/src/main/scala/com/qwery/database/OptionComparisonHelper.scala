package com.qwery.database

/**
 * Option Comparison Helper
 */
object OptionComparisonHelper {

  /**
   * Option Enrichment
   * @param optionA the host [[Field field]]
   */
  final implicit class OptionComparator(val optionA: Option[Any]) extends AnyVal {

    def >[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) > 0
          case _ => false
        }
      }).contains(true)
    }

    def >=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) >= 0
          case _ => false
        }
      }).contains(true)
    }

    def <[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) < 0
          case _ => false
        }
      }).contains(true)
    }

    def <=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA
        valueB <- optionB if valueA.getClass == valueB.getClass
      } yield {
        (valueA.asInstanceOf[AnyRef], valueB.asInstanceOf[AnyRef]) match {
          case (a: A, b: A) => a.compareTo(b) <= 0
          case _ => false
        }
      }).contains(true)
    }

  }

}
