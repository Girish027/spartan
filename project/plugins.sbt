logLevel := Level.Warn
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("com.github.mwz" % "sbt-sonar" % "2.1.0")

resolvers in ThisBuild += "Sonar Maven Repository" at " https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/com.github.mwz/sbt-sonar"
