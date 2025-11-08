ThisBuild / organization := "org.cscie88c"
// Align Scala version with AWS EMR Spark 3.5.x (built for Scala 2.12)
ThisBuild / scalaVersion := "2.12.19"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme := Some("semver-spec")

val circeVersion = "0.13.0"
val pureconfigVersion = "0.15.0"
val catsVersion = "2.2.0"

lazy val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test
)

lazy val commonDependencies = Seq(
  // cats FP libary
  "org.typelevel" %% "cats-core" % catsVersion,

  // support for JSON formats
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-literal" % circeVersion,

  // support for typesafe configuration
  "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,

  // logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
)

// common settings
lazy val commonSettings = Seq(
  // Use Scala 2.12 for Spark cluster compatibility
  scalaVersion := "2.12.19",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    // Adjust unused warnings depending on Scala binary version (2.13 vs 2.12)
    scalacOptions ++= {
      val base = Seq(
        "-feature",
        "-deprecation",
        "-unchecked",
        "-language:postfixOps",
        "-language:higherKinds",
        "-Yrangepos"
      )
      if (scalaBinaryVersion.value == "2.13") base ++ Seq("-Wunused", "-Wunused:imports")
      else base ++ Seq("-Ywarn-unused:imports") // closest equivalent for Scala 2.12
    },
    Compile / run / fork := true, // cleaner to run programs in a JVM different from sbt
    Compile / discoveredMainClasses := Seq(), // ignore discovered main classes
    // needed to run Spark with Java 17
    run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    ),
    libraryDependencies ++= commonDependencies,
    // Add parallel-collections only for Scala 2.13+ (not published for 2.12)
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.13")
        Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
      else Seq.empty
    },
    libraryDependencies ++= scalaTest,
)

lazy val dockerSettings = Seq(
  dockerBaseImage := "docker.io/library/eclipse-temurin:17-jre",
  Docker / version := "latest",
)

lazy val core = project
  .in(file("core"))
  .settings(
    commonSettings,
    // Core library settings
  )

lazy val cli = project
  .in(file("cli"))
  .settings(commonSettings, dockerSettings)
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)

lazy val spark = project
  .in(file("spark"))
  .settings(
    commonSettings,
    // Spark module settings)
  )
  .enablePlugins(JavaAppPackaging)

lazy val beam = project
  .in(file("beam"))
  .settings(
    commonSettings,
    // Beam module settings
  )
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)

lazy val root = (project in file("."))
  .aggregate(core, cli, spark, beam)
  .settings(
    commonSettings,
    name := "multi-module-project",
    description := "A multi-module project for Scala and Big data frameworks like Spark, Beam, and Kafka",
  )

// Custom task to zip files for homework submission
lazy val zipHomework = taskKey[Unit]("zip files for homework submission")

zipHomework := {
  val bd = baseDirectory.value
  val targetFile = s"${bd.getAbsolutePath}/scalaHomework.zip"
  val ignoredPaths =
    ".*(\\.idea|target|\\.DS_Store|\\.bloop|\\.metals|\\.vsc|\\.git|\\.devcontainer|\\.vscode|apps|setup|data)/*".r.pattern
  val fileFilter = new FileFilter {
    override def accept(f: File) =
      !ignoredPaths.matcher(f.getAbsolutePath).lookingAt
  }
  println("zipping homework files to scalaHomework.zip ...")
  IO.delete(new File(targetFile))
  IO.zip(
    Path.selectSubpaths(new File(bd.getAbsolutePath), fileFilter),
    new File(targetFile),
    None
  )
}