import sbt.*
import sbt.Keys.*

import com.typesafe.sbt.SbtNativePackager.{Docker, Linux}
import com.typesafe.sbt.packager.Keys.*
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.dockerLayerMappings
import com.typesafe.sbt.packager.docker.{DockerPlugin, LayeredMapping}
import com.typesafe.sbt.packager.linux.Mapper.packageTemplateMapping

object SbtDASPlugin extends AutoPlugin {

  // We require Docker + JavaAppPackaging so projects automatically get them.
  override def requires = DockerPlugin && JavaAppPackaging
  override def trigger = allRequirements

  object autoImport {
    // The only project-specific setting that we want devs to override in build.sbt
    val repoNameSetting = settingKey[String]("Repository/project name for Docker, publishing, etc.")
  }
  import autoImport._

  // Hardcoded organization name, username, etc., since everything else is "common"
  private val orgUsername = "raw-labs"
  private val orgName = "com.raw-labs"

  // We inline these...
  override def projectSettings: Seq[Setting[_]] = Seq(
    // Add GitHub credentials
    credentials += Credentials(
      "GitHub Package Registry",
      "maven.pkg.github.com",
      orgUsername,
      sys.env.getOrElse("GITHUB_TOKEN", "")),

    // Basic metadata
    homepage := Some(url("https://www.raw-labs.com/")),
    organization := orgName,
    organizationName := "RAW Labs SA",
    organizationHomepage := Some(url("https://www.raw-labs.com/")),

    // Use cached resolution
    updateOptions := updateOptions.in(Global).value.withCachedResolution(true),

    // Add local Maven + RAW Labs Package Registry resolvers
    resolvers ++= Seq(Resolver.mavenLocal, "RAW Labs GitHub Packages" at s"https://maven.pkg.github.com/raw-labs/_"),

    // We use Scala 2.13
    scalaVersion := "2.13.15",

    // Compile settings
    Compile / doc / sources := Seq.empty,
    Compile / packageDoc / mappings := Seq(),
    Compile / packageSrc / publishArtifact := true,
    Compile / packageDoc / publishArtifact := false,
    Compile / packageBin / packageOptions +=
      Package.ManifestAttributes("Automatic-Module-Name" -> name.value.replace('-', '.')),
    compileOrder := CompileOrder.JavaThenScala,
    Compile / run / fork := true,
    Compile / mainClass := Some("com.rawlabs.das.server.DASServer"),

    // Test settings
    Test / fork := true,
    Test / publishArtifact := true,

    // Make all warnings fatal
    scalacOptions ++= Seq("-Xfatal-warnings"),

    // Publish settings
    versionScheme := Some("early-semver"),
    publish / skip := false,
    publishMavenStyle := true,
    publishTo := {
      val repoName = repoNameSetting.value
      Some(s"GitHub $orgUsername Apache Maven Packages" at s"https://maven.pkg.github.com/$orgUsername/$repoName")
    },
    // Overwrite artifacts in CI
    publishConfiguration := {
      val isCI = sys.env.getOrElse("CI", "false").toBoolean
      publishConfiguration.value.withOverwrite(isCI)
    },

    // Let the `name` of this project follow our custom repoNameSetting
    name := repoNameSetting.value,

    // Docker settings
    Docker / packageName := s"${repoNameSetting.value}-server",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerLabels ++= Map(
      "vendor" -> "RAW Labs SA",
      "product" -> s"${repoNameSetting.value}-server",
      "image-type" -> "final",
      "org.opencontainers.image.source" -> s"https://github.com/$orgUsername/${repoNameSetting.value}"),
    Docker / daemonUser := "raw",
    Docker / daemonUserUid := Some("1001"),
    Docker / daemonGroup := "raw",
    Docker / daemonGroupGid := Some("1001"),
    dockerExposedVolumes := Seq("/var/log/raw"),
    dockerExposedPorts := Seq(50051),
    dockerEnvVars := Map("PATH" -> s"${(Docker / defaultLinuxInstallLocation).value}/bin:$$PATH", "LANG" -> "C.UTF-8"),
    updateOptions := updateOptions.value.withLatestSnapshots(true),

    // Make /var/lib/<packageName> be an explicit Linux package mapping
    Linux / linuxPackageMappings += packageTemplateMapping(s"/var/lib/${(Docker / packageName).value}")(),

    // Modify the bash script so we can prepend our conf folder
    bashScriptDefines := {
      val pattern = "declare -r app_classpath=\"(.*)\"\n".r
      bashScriptDefines.value.map {
        case pattern(cp) =>
          s"""
             |declare -r app_classpath="$${app_home}/../conf:$cp"
             |""".stripMargin
        case other => other
      }
    },

    // Put certain jars on the top layer for Docker layering
    Docker / dockerLayerMappings := {
      (Docker / dockerLayerMappings).value.map {
        case lm @ LayeredMapping(Some(1), file, path) =>
          val fileName = java.nio.file.Paths.get(path).getFileName.toString
          if (!fileName.endsWith(".jar")) {
            // If it is not a jar, put it on the top layer
            LayeredMapping(Some(2), file, path)
          } else if (fileName.startsWith("com.raw-labs") && fileName.endsWith(".jar")) {
            // Our own jars -> top layer
            LayeredMapping(Some(2), file, path)
          } else {
            // 3rd party jars stay in layer 1
            lm
          }
        case lm => lm
      }
    },

    // Clean up Docker version tag
    Docker / version := {
      val ver = version.value
      ver.replaceAll("[+]", "-").replaceAll("[^\\w.-]", "-")
    },

    // Let Docker push to GHCR by default
    dockerAlias := {
      val devRegistry = s"ghcr.io/$orgUsername/${repoNameSetting.value}"
      dockerAlias.value.withRegistryHost(Some(devRegistry))
    },

    // Define the printDockerImageName task
    printDockerImageName := {
      val alias = (Docker / dockerAlias).value
      println(s"DOCKER_IMAGE=$alias")
    })

  // A task key for printing the Docker name
  val printDockerImageName = taskKey[Unit]("Prints the full Docker image name that will be produced")
}
