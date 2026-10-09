scalaVersion := "3.10.0"

name := "hello-zio-bedrock"

libraryDependencies += "com.jamesward" %% "zio-bedrock" % "0.1.0"

fork := true

// sbt-mcp (loopback-only: its tools can execute build tasks)
ThisBuild / mcpEnabled := true
ThisBuild / mcpHost := "127.0.0.1"
ThisBuild / mcpPort := 5102

// SkillsJars: extract agent Skills with `./sbt extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

libraryDependencies += "com.jamesward" % "skills" % "0.0.11" % Skills

scalacOptions ++= Seq(
  "-language:strictEquality",
  "-deprecation",
  "-Werror",
)
