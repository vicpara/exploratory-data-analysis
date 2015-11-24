organization := "com.vicpara"

name := "exploratory-data-analysis"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

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
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

// Strat copied from defaultMergeStrategy with the "fail and confuse the hell out the user" lines changed to
// "just bloody work and stop pissing everyone off"
mergeStrategy in assembly <<= (mergeStrategy in assembly) ((old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    (xs map {
      _.toLowerCase
    }) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first // Changed deduplicate to first
    }
  case PathList(_*) => MergeStrategy.first // added this line
})
