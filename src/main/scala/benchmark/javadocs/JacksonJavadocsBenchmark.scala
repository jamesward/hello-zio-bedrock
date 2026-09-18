package benchmark.javadocs

import zio.json.ast.Json

object JacksonJavadocsBenchmark:
  val mcpUrl = "https://www.javadocs.dev/mcp"
  val artifactQuery = "jackson-databind"
  val topic = "polymorphic type validation"
  val syntheticToolDescription =
    "Recursively synthesize a schema-valid MCP workflow with Jev, execute it internally, and return compact evidence for a Maven library research goal."
  val goal =
    "summarize the classes in the latest jackson-databind library that are related to polymorphic type validation"

  val syntheticInputSchema: Json.Obj = Json.Obj(
    "type" -> Json.Str("object"),
    "properties" -> Json.Obj(
      "goal" -> Json.Obj("type" -> Json.Str("string"), "description" -> Json.Str("The complete research goal.")),
      "groupId" -> Json.Obj("type" -> Json.Str("string"), "description" -> Json.Str("Known Maven groupId.")),
      "artifactId" -> Json.Obj("type" -> Json.Str("string"), "description" -> Json.Str("Known Maven artifactId.")),
      "topic" -> Json.Obj("type" -> Json.Str("string"), "description" -> Json.Str("Topic used to semantically filter candidate API symbols.")),
    ),
    "required" -> Json.Arr(Json.Str("goal"), Json.Str("groupId"), Json.Str("artifactId"), Json.Str("topic")),
    "additionalProperties" -> Json.Bool(false),
  )

  val loopBaseInput: Json.Obj = Json.Obj(
    "goal" -> Json.Str(goal),
    "artifactQuery" -> Json.Str(artifactQuery),
    "topic" -> Json.Str(topic),
  )

  def finalGenerationPrompt(evidenceJson: String): String =
    s"""Answer the research goal using only the supplied evidence. Be concise but cover the important classes and their roles.
       |
       |Goal: $goal
       |
       |Evidence:
       |$evidenceJson""".stripMargin

  case class ResolvedInput(input: Json.Obj, toolCalls: Int, toolTimeMs: Long)

  def resolveInput(mcp: com.jamesward.ziohttp.mcp.client.McpClient): zio.Task[ResolvedInput] =
    for
      started <- zio.Clock.nanoTime
      result <- mcp.callTool("search_artifacts", Json.Obj("query" -> Json.Str(artifactQuery)))
      finished <- zio.Clock.nanoTime
      text = result.content.collect {
        case com.jamesward.ziohttp.mcp.ToolContent.Text(value, _) => value
      }.mkString("\n")
      _ <- zio.ZIO.fail(IllegalStateException(s"search_artifacts failed: $text")).when(result.isError.contains(true))
      artifacts <- zio.ZIO.fromOption(
        result.structuredContent.flatMap(_.asObject).flatMap(_.get("result")).flatMap(_.asArray)
      ).orElseFail(IllegalStateException("search_artifacts returned no structured result array"))
      artifact <- zio.ZIO.fromOption(
        artifacts.find(_.asObject.flatMap(_.get("artifactId")).flatMap(_.asString).contains(artifactQuery))
          .orElse(artifacts.headOption).flatMap(_.asObject)
      ).orElseFail(IllegalStateException("search_artifacts returned no artifact candidates"))
      groupId <- zio.ZIO.fromOption(artifact.get("groupId").flatMap(_.asString))
        .orElseFail(IllegalStateException("artifact candidate omitted groupId"))
      artifactId <- zio.ZIO.fromOption(artifact.get("artifactId").flatMap(_.asString))
        .orElseFail(IllegalStateException("artifact candidate omitted artifactId"))
      input = Json.Obj(loopBaseInput.fields ++ zio.Chunk(
        "groupId" -> Json.Str(groupId),
        "artifactId" -> Json.Str(artifactId),
      ))
    yield ResolvedInput(input, 1, (finished - started) / 1000000L)
