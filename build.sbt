import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-sqlite",
    libraryDependencies ++= Seq(
      // DAS
      "com.raw-labs" %% "das-server-scala" % "0.6.0" % "compile->compile;test->test",
      // Sqlite
      "org.xerial" % "sqlite-jdbc" % "3.49.1.0",
      // Jackson (for JSON handling)
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
      // Hikari Connection Pool,
      "com.zaxxer" % "HikariCP" % "6.2.1",
      // Mockito
      "org.mockito" % "mockito-core" % "5.12.0" % Test,
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % Test,
      // ScalaTest / containers
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.8" % Test),
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-handler" % "4.1.118.Final"
    ))
