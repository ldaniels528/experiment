package com.github.ldaniels528.qwery.actors

import akka.actor.Actor

import scala.concurrent.{ExecutionContext, Future}

/**
  * Actor Support
  * @author lawrence.daniels@gmail.com
  */
trait ActorSupport {
  self: Actor =>

  protected def reflect[A](block: => Future[A])(implicit ec: ExecutionContext): Unit = {
    val mySender = sender()
    block foreach (mySender ! _)
  }

}
