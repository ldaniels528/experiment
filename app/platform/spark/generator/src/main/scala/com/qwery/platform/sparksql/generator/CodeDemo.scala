package com.qwery.platform.sparksql.generator

object CodeDemo {

  def main(args: Array[String]): Unit = {
    val code = XNode("spark", prepend = "\n", append = "\n")(
      XNode(name = "SELECT", append = ",")(
        XNode("firstName", 1)(),
        XNode("lastName", 1)(),
        XNode("age", 1)()
      ),
      XNode("FROM", 0)(XNode(name = "Customers")()),
      XNode("WHERE", 0)(XNode(name = "customerId = 2")())
    )

    println(code.toCode)
  }

  case class XNode(name: String,
                   level: Int,
                   prepend: String,
                   append: String,
                   segments: List[XNode]) {

    def toCode: String = ("\t" * level) + name + prepend + segments.map(_.toCode).mkString(append)

  }

  object XNode {

    def apply(name: String,
              level: Int = 0,
              prepend: String = " ",
              append: String = " ")(segments: XNode*) = new XNode(name, level, prepend, append, segments.toList)
  }

}
