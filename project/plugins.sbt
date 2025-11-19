// 1. .proto 파일을 Scala 코드로 컴파일
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"

// 2. Fat JAR 파일
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")