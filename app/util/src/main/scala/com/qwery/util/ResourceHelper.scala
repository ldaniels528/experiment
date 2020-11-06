package com.qwery.util

import java.net.URL

import com.qwery.util.OptionHelper._

import scala.language.reflectiveCalls
import scala.reflect.ClassTag

/**
  * Resource Helper
  * @author lawrence.daniels@gmail.com
  */
object ResourceHelper {

  type Manageable = {
    def open(): Unit

    def close(): Unit
  }

  /**
    * Automatically closes a resource after the completion of a code block
    */
  final implicit class AutoOpenClose[T <: Manageable](val resource: T) extends AnyVal {

    def manage[S](block: T => S): S = try {
      resource.open()
      block(resource)
    } finally resource.close()

  }

  /**
    * Automatically closes a resource after the completion of a code block
    */
  final implicit class AutoClose[T <: {def close()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.close()

  }

  /**
    * Automatically closes a resource after the completion of a code block
    */
  final implicit class AutoDisconnect[T <: {def disconnect()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.disconnect()

  }

  /**
    * Automatically closes a resource after the completion of a code block
    */
  final implicit class AutoShutdown[T <: {def shutdown()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.shutdown()

  }

  /**
    * Type require utilities
    * @param entity a given entity
    * @tparam A the entities type
    */
  final implicit class TypeEnrichment[A](val entity: A) extends AnyVal {

    def require[B <: A](message: String)(implicit tag: ClassTag[B]): B = entity match {
      case entityB: B => entityB
      case _ => throw new IllegalArgumentException(message)
    }

  }

  final implicit class ClassPathResources(val name: String) extends AnyVal {

    def asURL: Option[URL] = Option(getClass.getClassLoader.getResource(name)) ?? Option(getClass.getResource(name))

  }

}
