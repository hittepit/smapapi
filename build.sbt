name := "SmapApi"

version := "0.0.1"

organization := "Hittepit Software"

scalaVersion := "2.10.3"

instrumentSettings

ScoverageKeys.excludedPackages in ScoverageCompile := "<empty>"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test->*" excludeAll (ExclusionRule(organization="org.junit", name="junit"))

libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5" % "test"

libraryDependencies += "com.h2database" % "h2" % "1.3.171" % "test"

scalacOptions ++= Seq("-feature", "-deprecation")

//(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/report")