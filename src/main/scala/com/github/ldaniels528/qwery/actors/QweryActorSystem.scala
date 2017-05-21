package com.github.ldaniels528.qwery.actors

import akka.actor._

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Qwery Actor Factory
  * @author lawrence.daniels@gmail.com
  */
class QweryActorSystem {
  val system = ActorSystem("qwery")
  val scheduler: Scheduler = system.scheduler

  def createActor[T <: Actor : ClassTag]: ActorRef = system.actorOf(Props[T])

  def createActor[T <: Actor : ClassTag](factory: () => T): ActorRef = system.actorOf(Props(factory()))

  def shutdown(): Future[Terminated] = system.terminate()

}

object QweryActorSystem extends QweryActorSystem