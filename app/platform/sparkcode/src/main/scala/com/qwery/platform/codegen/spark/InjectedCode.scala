package com.qwery.platform.codegen.spark

trait InjectedCode {
  def generate: String
}

case class ClassGen(name: String,
                    packageName: String,
                    code: Seq[InjectedCode],
                    imports: Seq[InjectedCode]) extends InjectedCode {
  override def generate: String = {
    s"""|package $packageName
        |
        |${imports.map(_.generate).mkString("\n")}
        |
        |class $name() extends Serializable {
        |   ${code.map(_.generate).mkString("\n")}
        |}
        |""".stripMargin
  }
}



