package com.github.ldaniels528.qwery.actors

import java.net.URL

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

/**
  * Qwery Actor System
  * @author lawrence.daniels@gmail.com
  */
class QweryActorSystem {
  val system = ActorSystem("qwery")
  val dispatcher: ExecutionContextExecutor = system.dispatcher
  val scheduler: Scheduler = system.scheduler
  val akkaConf: Config = getResource("/application.conf") match {
    case Some(url) => ConfigFactory.parseURL(url)
    case None => ConfigFactory.defaultApplication()
  }

  def createActor[T <: Actor : ClassTag]: ActorRef = system.actorOf(Props[T])

  def createActor[T <: Actor : ClassTag](name: String): ActorRef = system.actorOf(Props[T], name = name)

  def createActor[T <: Actor : ClassTag](factory: () => T): ActorRef = system.actorOf(Props(factory()))

  def createActor[T <: Actor : ClassTag](name: String, factory: () => T): ActorRef = {
    system.actorOf(Props(factory()), name = name)
  }

  def shutdown(): Future[Terminated] = system.terminate()

  private def getResource(path: String): Option[URL] = Option(getClass.getResource(path))

}
/**
  * Qwery Actor System Companion
  * @author lawrence.daniels@gmail.com
  */
object QweryActorSystem extends QweryActorSystem