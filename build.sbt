organization := "com.tfs.dp"

name := "spartan"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

resolvers += "Nexus Others" at "http://nexus.cicd.sv2.247-inc.net/nexus/repository/releases/"
resolvers += "Nexus Releases" at "http://nexus.cicd.sv2.247-inc.net/nexus/content/repositories/releases/"
resolvers += "Nexus Nightly" at "http://nexus.cicd.sv2.247-inc.net/nexus/content/repositories/nightly/"
resolvers += "Nexus Twitter" at "https://maven.twttr.com"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

libraryDependencies += "com.tfs.dp" % "spark-avro" % "promoted"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.3"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"
libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.58" % "provided"
libraryDependencies += "org.jasypt" % "jasypt" % "1.9.2"
libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.20"
libraryDependencies += "org.json" % "json" % "20171018"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.3.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.2"


libraryDependencies += "junit" % "junit"   % "4.11" % Test
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6+"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.0.0" % Test
libraryDependencies += "com.247.security" % "247.ai-CryptoService" % "0.0.8-release" % "provided"
libraryDependencies += "com.247.security" % "vault-java-driver" % "1.2.3-master-35" % "provided"


libraryDependencies ++= Seq("org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
    "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
    "org.apache.logging.log4j" % "log4j-core" % "2.8.2" % Runtime,
    "org.apache.logging.log4j" % "log4j-1.2-api" % "2.8.2",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.2")

libraryDependencies ++= Seq("com.tfs" % "tm-hadoop-binlog" % "1.13","com.tfs" % "binlog" % "1.3",
  "com._247.logging.binlog" % "tpc-binlog-transformer" % "1.0.4"  exclude ("org.apache.hadoop","hadoop-client") exclude ("org.apache.hadoop","hadoop-common"))