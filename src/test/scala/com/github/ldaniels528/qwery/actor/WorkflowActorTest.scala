package com.github.ldaniels528.qwery.actor

import com.github.ldaniels528.qwery.actors.WorkflowActor.CopyProcess
import com.github.ldaniels528.qwery.actors.{QweryActorSystem, WorkflowActor}
import com.github.ldaniels528.qwery.sources.DataResource
import org.scalatest.FunSpec

/**
  * Workflow Actor Test
  * @author lawrence.daniels@gmail.com
  */
class WorkflowActorTest extends FunSpec {

  describe("WorkflowActor") {

    it("should process files") {
      val workflow = QweryActorSystem.createActor[WorkflowActor]
      workflow ! CopyProcess(inputPath = DataResource("companylist.csv"), outputPath = DataResource("test4.json"))
      Thread.sleep(6000)
    }

  }

}
