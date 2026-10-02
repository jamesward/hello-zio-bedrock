scalaVersion := "3.9.0"

name := "hello-zio-bedrock"

libraryDependencies += "com.jamesward" %% "zio-bedrock-converse" % "0.0.1"

fork := true

// sbt-mcp (loopback-only: its tools can execute build tasks)
Global / mcpEnabled := true
Global / mcpHost := "127.0.0.1"
Global / mcpPort := 5102

// SkillsJars: extract agent Skills with `./sbt extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

libraryDependencies += "com.jamesward" % "skills" % "0.0.10" % Skills
