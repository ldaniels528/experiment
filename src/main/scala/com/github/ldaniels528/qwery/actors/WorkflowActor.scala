package com.github.ldaniels528.qwery.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.github.ldaniels528.qwery.actors.ResourceReadingActor.{DataReceived, EOF, ReadFile}
import com.github.ldaniels528.qwery.actors.WorkflowActor._
import com.github.ldaniels528.qwery.ops.{RootScope, Row}
import com.github.ldaniels528.qwery.sources.{DataResource, OutputSource}

import scala.collection.concurrent.TrieMap

/**
  * Workflow Actor
  * @author lawrence.daniels@gmail.com
  */
class WorkflowActor() extends Actor with ActorLogging {

  override def receive: Receive = {
    case op@CopyProcess(inputPath, outputPath, pid) =>
      log.info(s"$pid: Copy process started ('$inputPath' to '$outputPath')")
      jobs(pid) = op.start(self)

    case DataReceived(pid, row) =>
      jobs.get(pid) foreach (_.write(row))

    case EOF(pid, path) =>
      log.info(s"$pid: Process completed reading '$path'")
      jobs.remove(pid) foreach (_.close())

    case message =>
      unhandled(message)
  }
}

/**
  * Workflow Actor
  * @author lawrence.daniels@gmail.com
  */
object WorkflowActor {
  private val jobs = TrieMap[UUID, CopyProcess]()

  case class CopyProcess(inputPath: DataResource, outputPath: DataResource, pid: UUID = UUID.randomUUID()) {
    private var output: OutputSource = _
    private var reader: ActorRef = _

    def close(): Unit = output.close()

    def start(actor: ActorRef): this.type = {
      // open the output source
      output = outputPath.getOutputSource(append = false)
        .getOrElse(throw new IllegalArgumentException(s"No output device found for '$outputPath'"))
      output.open(RootScope())

      // start read from the input source
      reader = QweryActorSystem.createActor[ResourceReadingActor]
      reader ! ReadFile(pid, inputPath, recipient = actor)
      this
    }

    def write(row: Row): Unit = output.write(row)
  }

}
