// Code Coverage

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

// DataBricks / Spark

addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.5")

// Publishing

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")

// ScalaJS

resolvers += Resolver.url("scala-js-snapshots", url("http://repo.scala-js.org/repo/snapshots/"))(Resolver.ivyStylePatterns)

// Typesafe

addSbtPlugin("com.typesafe.akka" % "akka-sbt-plugin" % "2.2.3")
