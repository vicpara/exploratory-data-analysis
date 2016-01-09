import com.typesafe.sbt.SbtScalariform

sonatypeProfileName := "com.github.vicpara"

organization := "com.github.vicpara"

name := "exploratory-data-analysis"

releaseVersionFile := file("version.sbt")

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.11.7", "2.10.4")

publishMavenStyle := true

pomIncludeRepository := { _ => false }

homepage := Some(url("https://github.com/vicpara/exploratory-data-analysis"))

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("-SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.6" withSources() withJavadoc(),
  "org.joda" % "joda-convert" % "1.2" withSources() withJavadoc(),
  "org.apache.spark" % "spark-core_2.10" % "1.3.0-cdh5.4.4" withSources() withJavadoc(),
  "org.apache.commons" % "commons-csv" % "1.2" withSources() withJavadoc(),
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3" withSources() withJavadoc(),
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3" withSources() withJavadoc(),
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.3" withSources() withJavadoc(),
  "org.scalaz" %% "scalaz-core" % "7.0.6" withSources() withJavadoc(),
  "org.rogach" %% "scallop" % "0.9.5" withSources() withJavadoc(),
  "org.scala-lang" % "scalap" % "2.10.4" withSources() withJavadoc(),
  "org.scala-lang" % "scala-compiler" % "2.10.4" withSources() withJavadoc(),
  "com.github.tototoshi" %% "scala-csv" % "1.2.2" withSources() withJavadoc(),
  "org.specs2" %% "specs2-core" % "2.4.9-scalaz-7.0.6" % "test" withSources() withJavadoc(),
  "org.specs2" %% "specs2-scalacheck" % "2.4.9-scalaz-7.0.6" % "test" withSources() withJavadoc(),
  "io.spray" %% "spray-json" % "1.3.1" withSources() withJavadoc(),
  "org.scalaj" %% "scalaj-http" % "1.1.5" withSources() withJavadoc(),
  "io.continuum.bokeh" %% "bokeh" % "0.6" withSources() withJavadoc()
)

resolvers ++= Seq(
  "mvnrepository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
)

pomExtra :=
  <url>http://github.com/vicpara/exploratory-data-analysis/</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:vicpara/exploratory-data-analysis.git</url>
      <connection>scm:git:git@github.com:vicpara/exploratory-data-analysis.git</connection>
    </scm>
    <developers>
      <developer>
        <id>vicpara</id>
        <name>Victor Paraschiv</name>
        <url>http://github.com/vicpara</url>
      </developer>
    </developers>

scalariformSettings

