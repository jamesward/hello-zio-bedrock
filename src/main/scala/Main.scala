import benchmark.javadocs.JacksonJavadocsBenchmark as Benchmark
import com.jamesward.zio_bedrock_converse.Bedrock
import com.jamesward.zio_bedrock_converse.Bedrock.*
import com.jamesward.zio_typesafe_ai.TypeSafeAI
import com.jamesward.ziohttp.mcp.{CallToolResult, ToolDefinition}
import com.jamesward.ziohttp.mcp.client.McpClient
import zio.*
import zio.http.Client as HttpClient
import zio.json.*
import zio.json.ast.Json

object Main extends ZIOAppDefault:

  private val McpUrl = Benchmark.mcpUrl
  private val Prompt = Benchmark.goal

  private val syntheticTool = Tool.dynamic(
    ToolName("research_with_jev"),
    Benchmark.syntheticToolDescription,
    Benchmark.syntheticInputSchema,
  )

  private def requiredString(input: Json.Obj, field: String): Task[String] =
    ZIO.fromOption(input.get(field).flatMap(_.asString))
      .orElseFail(IllegalArgumentException(s"Missing string field '$field'"))

  private def printMetrics(result: DynamicLoopResult[String], label: String): UIO[Unit] =
    ZIO.foreachDiscard(result.turns): turn =>
      Console.printLine(
        s"[$label:bedrock] turn=${turn.turn} stop=${turn.stopReason} inputTokens=${turn.usage.inputTokens} " +
          s"outputTokens=${turn.usage.outputTokens} tools=[${turn.toolNames.map(_.unwrap).mkString(", ")}]"
      ).orDie
    *> Console.printLine(s"\n${result.output}").orDie
    *> Console.printLine(
      s"\n[$label] Completed ${result.turns.size} Bedrock turns; " +
        s"inputTokens=${result.totals.usage.inputTokens}, " +
        s"outputTokens=${result.totals.usage.outputTokens}, " +
        s"totalTokens=${result.totals.usage.totalTokens}, " +
        s"latencyMs=${result.totals.latencyMs}"
    ).orDie


  private case class ToolTiming(calls: Int, summedMs: Long, wallMs: Long)

  private def summarizeIntervals(intervals: List[(Long, Long)]): ToolTiming =
    val sorted = intervals.sortBy(_._1)
    val merged = sorted.foldLeft(List.empty[(Long, Long)]):
      case (Nil, interval) => List(interval)
      case ((start, end) :: tail, (nextStart, nextEnd)) if nextStart <= end =>
        (start, math.max(end, nextEnd)) :: tail
      case (acc, interval) => interval :: acc
    ToolTiming(
      intervals.size,
      intervals.map((start, end) => (end - start) / 1000000L).sum,
      merged.map((start, end) => (end - start) / 1000000L).sum,
    )
  // ----- Original baseline: every MCP operation is directly model-visible. -----

  private def rawMcpContent(result: CallToolResult): String = result.content.toJson

  private def invokeRaw(
    client: McpClient,
    name: String,
    arguments: Json.Obj,
    intervals: Ref[List[(Long, Long)]],
  ): Task[String] =
    for
      _ <- Console.printLine(s"[original:mcp] -> $name ${arguments.toJson}").orDie
      started <- Clock.nanoTime
      result <- client.callTool(name, arguments)
      finished <- Clock.nanoTime
      _ <- intervals.update((started -> finished) :: _)
      output = rawMcpContent(result)
      _ <- Console.printLine(s"[original:mcp] <- $name (${output.length} chars)").orDie
      _ <- ZIO.fail(RuntimeException(s"MCP tool $name failed: $output")).when(result.isError.contains(true))
    yield output

  private def originalTools(definitions: Chunk[ToolDefinition]): Task[List[Tool[?]]] =
    val names = definitions.map(_.name.value).toList
    val duplicates = names.groupMapReduce(identity)(_ => 1)(_ + _).collect:
      case (name, count) if count > 1 => name
    if duplicates.nonEmpty then ZIO.fail(IllegalArgumentException(s"Duplicate MCP tools: ${duplicates.mkString(", ")}"))
    else ZIO.succeed(definitions.toList.map: definition =>
      Tool.dynamic(
        ToolName(definition.name.value),
        definition.description.getOrElse(definition.name.value),
        definition.inputSchema,
      )
    )

  private val originalProgram = ZIO.scoped:
    for
      started <- Clock.nanoTime
      intervals <- Ref.make(List.empty[(Long, Long)])
      mcp <- McpClient.connect(McpUrl)
      definitions <- mcp.listTools
      tools <- originalTools(definitions)
      available = definitions.map(_.name.value).toSet
      _ <- Console.printLine(s"[original] Exposing ${tools.size} raw MCP tools to Bedrock.")
      result <- Bedrock.dynamicLoop(Prompt, tools) { (name, input) =>
        val toolName = name.unwrap
        for
          _ <- ZIO.fail(IllegalArgumentException(s"Unknown MCP tool: $toolName")).unless(available.contains(toolName))
          arguments <- ZIO.fromEither(input.asJsonObject).mapError(IllegalArgumentException(_))
          output <- invokeRaw(mcp, toolName, arguments, intervals)
        yield DynamicToolResult.text(output)
      }.maxIterations(20).text.provideSomeLayer[HttpClient](Bedrock.Client.live)
      finished <- Clock.nanoTime
      recorded <- intervals.get
      toolsTiming = summarizeIntervals(recorded)
      totalTimeMs = (finished - started) / 1000000L
      bedrockTimeMs = result.turns.map(_.metrics.latencyMs).sum
      orchestrationTimeMs = math.max(0L, totalTimeMs - bedrockTimeMs - toolsTiming.wallMs)
      _ <- printMetrics(result, "original")
      _ <- Console.printLine(
        s"[original] timing: bedrockTimeMs=$bedrockTimeMs, toolCalls=${toolsTiming.calls}, " +
          s"toolTimeMs=${toolsTiming.wallMs}, summedToolTimeMs=${toolsTiming.summedMs}, " +
          s"orchestrationTimeMs=$orchestrationTimeMs, totalTimeMs=$totalTimeMs"
      )
    yield ()

  // ----- Jev synthetic mode: one model-visible tool, internal plan + MCP. -----

  private def jevSynthProgram(strategy: JavadocsWorkflowPlanner.Strategy) = ZIO.scoped:
    val strategyName = strategy.toString.toLowerCase
    for
      started <- Clock.nanoTime
      executionMetrics <- Ref.make(List.empty[JavadocsWorkflowExecutor.ExecutionMetrics])
      mcp <- McpClient.connect(McpUrl)
      definitions <- mcp.listTools
      _ <- Console.printLine(
        s"[jev-synth:$strategyName] Exposing one recursive Jev synthetic tool over ${definitions.size} MCP operations."
      )
      runAndUsage <- (for
        result <- Bedrock.dynamicLoop(Prompt, List(syntheticTool)) { (name, toolInput) =>
          for
            _ <- ZIO.fail(IllegalArgumentException(s"Unknown synthetic tool: ${name.unwrap}"))
              .unless(name == syntheticTool.name)
            input <- ZIO.fromEither(toolInput.asJsonObject).mapError(IllegalArgumentException(_))
            goal <- requiredString(input, "goal")
            topic <- requiredString(input, "topic")
            _ <- requiredString(input, "groupId")
            _ <- requiredString(input, "artifactId")
            workflow <- JavadocsWorkflowPlanner.synthesize(
              goal,
              topic,
              input,
              definitions,
              strategy = strategy,
            )
            _ <- Console.printLine(s"\n[jev-synth:$strategyName] workflow:\n${workflow.toJson.toJsonPretty}")
            execution <- JavadocsWorkflowExecutor.execute(
              workflow,
              input,
              goal,
              JavadocsWorkflowExecutor.ToolInvoker.fromMcp(mcp),
            )
            _ <- executionMetrics.update(execution.metrics :: _)
            evidence = execution.output
            _ <- Console.printLine(s"[jev-synth:$strategyName] compact evidence: ${evidence.toJson.length} chars")
          yield DynamicToolResult.text(evidence.toJson)
        }.maxIterations(3).text
        jevUsage <- ZIO.serviceWithZIO[JavadocsWorkflowPlanner.WorkflowJudge](_.usage)
      yield result -> jevUsage).provideSomeLayer[HttpClient](
        Bedrock.Client.live ++ (TypeSafeAI.Client.live >>> JavadocsWorkflowPlanner.WorkflowJudge.jev)
      )
      (result, jevUsage) = runAndUsage
      finished <- Clock.nanoTime
      metrics <- executionMetrics.get
      toolCalls = metrics.map(_.toolCalls).sum
      toolTimeMs = metrics.map(_.toolTimeMs).sum
      summedToolTimeMs = metrics.map(_.summedToolTimeMs).sum
      bedrockTimeMs = result.totals.latencyMs
      totalTimeMs = (finished - started) / 1000000L
      orchestrationTimeMs = math.max(0L, totalTimeMs - bedrockTimeMs - jevUsage.timeMs - toolTimeMs)
      _ <- printMetrics(result, s"jev-synth:$strategyName")
      _ <- Console.printLine(
        s"[jev-synth:$strategyName] metrics: " +
          s"bedrockTurns=${result.turns.size}, bedrockInputTokens=${result.totals.usage.inputTokens}, " +
          s"bedrockOutputTokens=${result.totals.usage.outputTokens}, bedrockTotalTokens=${result.totals.usage.totalTokens}, " +
          s"jevTurns=${jevUsage.turns}, jevInputTokens=${jevUsage.inputTokens}, " +
          s"jevOutputTokens=${jevUsage.outputTokens}, jevTotalTokens=${jevUsage.totalTokens}, " +
          s"bedrockTimeMs=$bedrockTimeMs, jevTimeMs=${jevUsage.timeMs}, " +
          s"toolCalls=$toolCalls, toolTimeMs=$toolTimeMs, summedToolTimeMs=$summedToolTimeMs, " +
          s"orchestrationTimeMs=$orchestrationTimeMs, totalTimeMs=$totalTimeMs"
      )
    yield ()


  // ----- Flipped mode: Jev owns the loop; Bedrock only generates final prose. -----

  private val jevLoopProgram = ZIO.scoped:
    for
      started <- Clock.nanoTime
      mcp <- McpClient.connect(McpUrl)
      resolution <- Benchmark.resolveInput(mcp)
      jevLoopInput = resolution.input
      definitions <- mcp.listTools
      planning <- JavadocsWorkflowPlanner.synthesizeWithTypeSafeLoop(
        Prompt,
        Benchmark.topic,
        jevLoopInput,
        definitions,
      ).provideSomeLayer[HttpClient](TypeSafeAI.Client.live)
      _ <- Console.printLine(
        s"[jev-loop] planned in ${planning.turns.size} Jev turns; " +
          s"inputTokens=${planning.usage.inputTokens}, outputTokens=${planning.usage.outputTokens}"
      )
      _ <- Console.printLine(s"[jev-loop] workflow:\n${planning.output.toJson.toJsonPretty}")
      execution <- (for
        executionResult <- JavadocsWorkflowExecutor.execute(
          planning.output,
          jevLoopInput,
          Prompt,
          JavadocsWorkflowExecutor.ToolInvoker.fromMcp(mcp),
          JavadocsWorkflowExecutor.ExecutionPolicy(unbounded = true),
        )
        filteringUsage <- ZIO.serviceWithZIO[JavadocsWorkflowPlanner.WorkflowJudge](_.usage)
      yield executionResult -> filteringUsage).provideSomeLayer[HttpClient](
        TypeSafeAI.Client.live >>> JavadocsWorkflowPlanner.WorkflowJudge.jev
      )
      (executionResult, filteringUsage) = execution
      evidence = executionResult.output
      evidenceJson = evidence.toJson
      _ <- Console.printLine(s"[jev-loop] compact evidence: ${evidenceJson.length} chars")
      finalPrompt = Benchmark.finalGenerationPrompt(evidenceJson)
      bedrockStarted <- Clock.nanoTime
      generated <- Bedrock.converse(finalPrompt).asResponse
        .provideSomeLayer[HttpClient](Bedrock.Client.live)
      bedrockFinished <- Clock.nanoTime
      totalMillis = (bedrockFinished - started) / 1000000L
      bedrockTimeMs = (bedrockFinished - bedrockStarted) / 1000000L
      jevTurns = planning.turns.size + filteringUsage.turns
      jevInputTokens = planning.usage.inputTokens + filteringUsage.inputTokens
      jevOutputTokens = planning.usage.outputTokens + filteringUsage.outputTokens
      jevTotalTokens = jevInputTokens + jevOutputTokens
      jevTimeMs = planning.latencyMs + filteringUsage.timeMs
      searchTimeMs = resolution.toolTimeMs
      toolCalls = executionResult.metrics.toolCalls + resolution.toolCalls
      toolTimeMs = executionResult.metrics.toolTimeMs + searchTimeMs
      summedToolTimeMs = executionResult.metrics.summedToolTimeMs + searchTimeMs
      orchestrationTimeMs = math.max(0L, totalMillis - bedrockTimeMs - jevTimeMs - toolTimeMs)
      _ <- Console.printLine(s"\n${generated.output.text}")
      _ <- Console.printLine(
        s"\n[jev-loop] metrics: " +
          s"bedrockTurns=1, bedrockInputTokens=${generated.usage.inputTokens}, " +
          s"bedrockOutputTokens=${generated.usage.outputTokens}, bedrockTotalTokens=${generated.usage.totalTokens}, " +
          s"jevTurns=$jevTurns, jevInputTokens=$jevInputTokens, " +
          s"jevOutputTokens=$jevOutputTokens, jevTotalTokens=$jevTotalTokens, " +
          s"bedrockTimeMs=$bedrockTimeMs, jevTimeMs=$jevTimeMs, " +
          s"toolCalls=$toolCalls, toolTimeMs=$toolTimeMs, summedToolTimeMs=$summedToolTimeMs, " +
          s"orchestrationTimeMs=$orchestrationTimeMs, totalTimeMs=$totalMillis"
      )
    yield ()
  def run = (ZIOAppArgs.getArgs.flatMap:
    case Chunk("original")                => originalProgram
    case Chunk("jev-synth")               => jevSynthProgram(JavadocsWorkflowPlanner.Strategy.Backward)
    case Chunk("jev-synth", "backward")   => jevSynthProgram(JavadocsWorkflowPlanner.Strategy.Backward)
    case Chunk("jev-synth", "forward")    => jevSynthProgram(JavadocsWorkflowPlanner.Strategy.Forward)
    case Chunk("jev-loop")                => jevLoopProgram
    case args =>
      Console.printLineError(
        s"Usage: run original | run jev-synth [backward|forward] | run jev-loop; received: ${args.mkString(" ")}"
      ) *> ZIO.fail(IllegalArgumentException("Select original, Jev synthesis, or Jev loop mode"))
  ).provideSomeLayer[ZIOAppArgs](HttpClient.default)
