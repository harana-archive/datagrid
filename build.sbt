import java.nio.charset.Charset
import java.nio.file.{Files, StandardCopyOption}

import com.typesafe.config.ConfigFactory
import sbt.{File, _}
import scalajsbundler.sbtplugin.ScalaJSBundlerPlugin.autoImport.npmDependencies

val utf8 = Charset.forName("UTF-8")
val fastCompile = TaskKey[Unit]("fastCompile")
val fullCompile = TaskKey[Unit]("fullCompile")
val config = ConfigFactory.parseFile(new File("jvm/src/main/resources/application.conf"))
val httpPort = config.getInt("http.listenPort")

lazy val datagrid = project.in(file(".")).
	aggregate(crossProject.js, crossProject.jvm).
	settings(
		publish := {},
		publishLocal := {}
	)

lazy val crossProject = haranaCrossProject("datagrid").in(file("."))
  .enablePlugins(JavaServerAppPackaging)
	.enablePlugins(DockerPlugin)
	.jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin).enablePlugins(TzdbPlugin))
	.jvmConfigure(_.enablePlugins(DockerPlugin))
  .settings(
    name := "datagrid",
    version := "0.0.1",
    libraryDependencies ++= Seq(
			"modules" %%% "modules" % "d",
			"com.softwaremill.sttp.client" %%% "core" % "2.2.1",
			"com.softwaremill.sttp.client" %%% "circe" % "2.2.1",
			"com.softwaremill.quicklens" %%% "quicklens" % "1.6.0",
			"io.suzaku" %%% "diode" % "1.1.11"
		)
	).jsSettings(
		zonesFilter := {(z: String) => z == "Australia/Sydney" || z == "Pacific/Honolulu"},
		fastCompile := { copyJS(baseDirectory).dependsOn((Compile / fastOptJS / webpack)) }.value,
		fullCompile := { copyJS(baseDirectory).dependsOn((Compile / fullOptJS / webpack)) }.value,
		npmDependencies in Compile ++= Seq()
  ).jvmSettings(
		libraryDependencies ++= Seq(
			"datagrid-common"		%%   "datagrid-common"		% "0.0.12",
			"org.junit.jupiter" % "junit-jupiter" % "5.6.2" % "test"
		)
	)

onLoad in Global := (onLoad in Global).value andThen (Command.process("project datagridJVM;", _))

def copyJS(baseDirectory: SettingKey[File]) = {
	baseDirectory map {
		base =>
			new File(base, "target/scala-2.12/scalajs-bundler/main").listFiles((dir, name) => name.toLowerCase.contains("opt"))
				.foreach(
					file => Files.copy(file.toPath, new File(base, s"../public/js/${file.getName}").toPath, StandardCopyOption.REPLACE_EXISTING)
				)
	}
}
