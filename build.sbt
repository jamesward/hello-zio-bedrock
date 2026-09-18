scalaVersion := "3.9.0"

name := "hello-zio-bedrock"

libraryDependencies ++= Seq(
  "com.jamesward" %% "zio-bedrock-converse" % "0.1.1",
  "com.jamesward" %% "zio-http-mcp" % "0.8.2",
  "com.jamesward" %% "zio-typesafe-ai" % "0.0.2",
  "dev.zio" %% "zio-test" % "2.1.26" % Test,
  "dev.zio" %% "zio-test-sbt" % "2.1.26" % Test,
)

fork := true
