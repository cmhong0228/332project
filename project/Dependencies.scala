import sbt._

object Dependencies {
  val munitVersion = "1.0.0"
  val configVersion = "1.4.3"
  val scoptVersion = "4.1.0"

  val grpcDependencies = Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

  val testDependencies = Seq(
    "org.scalameta" %% "munit" % munitVersion % Test
  )

  val configDependencies = Seq(
    "com.typesafe" % "config" % configVersion
  )

  val scoptDependencies = Seq(
    "com.github.scopt" %% "scopt" % scoptVersion
  )

  val projectDependencies = grpcDependencies ++ testDependencies ++ configDependencies ++ scoptDependencies
}
