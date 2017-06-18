package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.sql.InnerJoin
import com.github.ldaniels528.qwery.sources.{DataResource, NamedResource, OutputSource}

/**
  * Created by ldaniels on 6/17/17.
  */
object JoinDemo extends App {
  val scope = RootScope()

  OutputSource("test.csv") foreach { output =>
    output.open(scope)
    InnerJoin(
      left = NamedResource(name = "A", DataResource("companylist.csv")),
      right = NamedResource(name = "B", DataResource("companylist2.csv")),
      condition = EQ(JoinColumnRef("A", "Symbol"), JoinColumnRef("B", "Symbol")))
      .execute(scope) foreach output.write
    output.close()
  }

}
