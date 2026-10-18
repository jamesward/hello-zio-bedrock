scalaVersion := "3.10.0"

name := "hello-zio-bedrock"

libraryDependencies += "com.jamesward" %% "zio-bedrock" % "0.1.0"

fork := true

// SkillsJars: extract agent Skills with `./sbta extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

libraryDependencies += "com.jamesward" % "skills" % "0.0.14" % Skills

scalacOptions ++= Seq(
  "-language:strictEquality",
  "-deprecation",
  "-Werror",
)
