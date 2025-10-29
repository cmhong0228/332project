import Dependencies._

//import com.thesamet.scalapb.sbt.ScalapbPlugin.autoImport._
//import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion     := "2.13.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
 // .enablePlugins(ScalapbPlugin, AssemblyPlugin)
  .settings(

    //Compile / scalacOptions += "-Xdisable-assertions"

    libraryDependencies ++= Dependencies.projectDependencies,
    
    Compile / PB.protoSources := Seq(baseDirectory.value / "src/main/proto"),
    
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),

    assembly / assemblyJarName := "distributedsorting.jar" ,

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
