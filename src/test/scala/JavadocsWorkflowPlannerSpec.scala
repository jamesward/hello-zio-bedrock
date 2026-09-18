import com.jamesward.ziohttp.mcp.{CallToolResult, ToolDefinition, ToolName}
import zio.*
import zio.json.ast.Json
import zio.test.*

object JavadocsWorkflowPlannerSpec extends ZIOSpecDefault:
  import JavadocsWorkflowPlanner.*

  private val string = Json.Obj("type" -> Json.Str("string"))
  private def obj(required: List[String], fields: (String, Json)*): Json.Obj = Json.Obj(
    "type" -> Json.Str("object"),
    "properties" -> Json.Obj(fields*),
    "required" -> Json.Arr(required.map(Json.Str(_))*),
  )

  private val groupArtifact = obj(List("groupId", "artifactId"), "groupId" -> string, "artifactId" -> string)
  private val versioned = obj(
    List("groupId", "artifactId", "version"),
    "groupId" -> string, "artifactId" -> string, "version" -> string,
  )
  private val page = obj(
    List("groupId", "artifactId", "version", "link"),
    "groupId" -> string, "artifactId" -> string, "version" -> string, "link" -> string,
  )

  private val definitions = Chunk(
    ToolDefinition(
      ToolName("get_latest_version"), Some("Resolve the latest artifact version"), groupArtifact,
      Some(obj(List("result"), "result" -> string)),
    ),
    ToolDefinition(
      ToolName("get_javadoc_index"), Some("Read one top-level index"), versioned,
      Some(obj(List("result"), "result" -> string)),
    ),
    ToolDefinition(
      ToolName("list_javadoc_symbols"), Some("List public classes with fqn and link"), versioned,
      Some(obj(
        List("result"),
        "result" -> Json.Obj(
          "type" -> Json.Str("array"),
          "items" -> obj(List("fqn", "link"), "fqn" -> string, "link" -> string, "kind" -> string),
        ),
      )),
    ),
    ToolDefinition(
      ToolName("get_javadoc_symbol"), Some("Read rendered docs using a symbol link"), page,
      Some(obj(List("result"), "result" -> string)),
    ),
    ToolDefinition(
      ToolName("get_source_file"), Some("Read source using a file link"), page,
      Some(obj(List("result"), "result" -> string)),
    ),
  )

  private final case class ScriptedJudge(ref: Ref[List[String]]) extends WorkflowJudge:
    def usage: UIO[JevUsage] = ZIO.succeed(JevUsage.empty)

    def choose(goal: String, partial: Json, candidates: NonEmptyChunk[Candidate]): Task[String] =
      ref.modify:
        case expected :: rest => (Some(expected), rest)
        case Nil              => (None, Nil)
      .flatMap(expected => ZIO.fromOption(expected).orElseFail(IllegalStateException("script exhausted")))
      .flatMap: expected =>
        ZIO.fromOption(candidates.find(_.key == expected).map(_.id))
          .orElseFail(IllegalStateException(s"Expected candidate '$expected'; got ${candidates.map(_.key).mkString(", ")}"))

    def relevant(goal: String, items: Chunk[Json], keepLimit: Int): Task[Chunk[Int]] =
      val matching = items.zipWithIndex.collect:
        case (item, index) if item.toString.toLowerCase.contains("polymorphic") => index
      ZIO.succeed(matching.take(keepLimit))

  private val judgeLayer = ZLayer.fromZIO:
    Ref.make(List(
      "call:get_latest_version",
      "call:list_javadoc_symbols",
      "filter:list_javadoc_symbols",
      "fanout:get_javadoc_symbol",
      "finish",
    )).map(ScriptedJudge(_))

  private val backwardJudgeLayer = ZLayer.fromZIO:
    Ref.make(List(
      "backward:rendered public API documentation for named classes; implementation source is not desired:get_javadoc_symbol",
      "backward:collection whose item fields satisfy the detail tool inputs:list_javadoc_symbols",
      "backward:scalar prerequisite required by the collection tool:get_latest_version",
    )).map(ScriptedJudge(_))

  private val input = Json.Obj(
    "goal" -> Json.Str("Summarize classes related to polymorphic type validation"),
    "groupId" -> Json.Str("com.fasterxml.jackson.core"),
    "artifactId" -> Json.Str("jackson-databind"),
    "topic" -> Json.Str("polymorphic type validation"),
  )

  def spec = suite("recursive Jev-guided workflow synthesis")(
    test("schema-valid transitions compile into call/filter/fanout AST") {
      JavadocsWorkflowPlanner.synthesize(
        "Summarize public classes related to polymorphic type validation",
        "polymorphic type validation",
        input,
        definitions,
      ).provideLayer(judgeLayer).map: workflow =>
        val names = workflow.steps.map:
          case Step.Call(_, tool, _)      => s"call:$tool"
          case Step.Filter(_, _, _, _, _) => "filter"
          case Step.FanOut(_, _, tool, _, _) => s"fanout:$tool"
        val versionBinding = workflow.steps.collectFirst:
          case Step.Call(_, "list_javadoc_symbols", arguments) => arguments("version")
        val linkBinding = workflow.steps.collectFirst:
          case Step.FanOut(_, _, "get_javadoc_symbol", arguments, _) => arguments("link")
        assertTrue(
          names == Vector(
            "call:get_latest_version",
            "call:list_javadoc_symbols",
            "filter",
            "fanout:get_javadoc_symbol",
          ),
          versionBinding.contains(Expr.Ref("n1", List("result"))),
          linkBinding.contains(Expr.Item(List("link"))),
          workflow.result == Expr.Ref("n4"),
        )
    },
    test("backward synthesis resolves evidence dependencies into the same AST") {
      JavadocsWorkflowPlanner.synthesize(
        "Summarize public classes related to polymorphic type validation",
        "polymorphic type validation",
        input,
        definitions,
        strategy = Strategy.Backward,
      ).provideLayer(backwardJudgeLayer).map: workflow =>
        val tools = workflow.steps.collect:
          case Step.Call(_, tool, _)         => tool
          case Step.FanOut(_, _, tool, _, _) => tool
        assertTrue(
          tools == Vector("get_latest_version", "list_javadoc_symbols", "get_javadoc_symbol"),
          workflow.steps.exists(_.isInstanceOf[Step.Filter]),
          workflow.result == Expr.Ref("n4"),
        )
    },
    test("synthesized AST executes resolver, filter, and bounded fan-out") {
      for
        calls <- Ref.make(List.empty[(String, Json.Obj)])
        invoker = new JavadocsWorkflowExecutor.ToolInvoker:
          def call(name: String, arguments: Json.Obj): Task[CallToolResult] =
            calls.update(_ :+ (name -> arguments)) *>
              (name match
                case "get_latest_version" =>
                  ZIO.succeed(CallToolResult(structuredContent = Some(Json.Obj("result" -> Json.Str("2.22.2")))))
                case "list_javadoc_symbols" =>
                  val noise = (0 until 45).map: index =>
                    Json.Obj("fqn" -> Json.Str(s"UnrelatedClass$index"), "link" -> Json.Str(s"noise$index.html"))
                  val relevant = Seq(
                    Json.Obj("fqn" -> Json.Str("PolymorphicTypeValidator"), "link" -> Json.Str("ptv.html")),
                    Json.Obj("fqn" -> Json.Str("BasicPolymorphicTypeValidator"), "link" -> Json.Str("basic.html")),
                  )
                  ZIO.succeed(CallToolResult(structuredContent = Some(Json.Obj(
                    "result" -> Json.Arr((noise ++ relevant)*)
                  ))))
                case "get_javadoc_symbol" =>
                  val link = arguments.get("link").flatMap(_.asString).getOrElse("missing")
                  ZIO.succeed(CallToolResult(structuredContent = Some(Json.Obj("result" -> Json.Str(s"docs:$link")))))
                case other => ZIO.fail(IllegalStateException(s"unexpected tool $other")))
        output <- (for
          workflow <- JavadocsWorkflowPlanner.synthesize(
            "Summarize public classes related to polymorphic type validation",
            "polymorphic type validation",
            input,
            definitions,
          )
          result <- JavadocsWorkflowExecutor.execute(workflow, input, "polymorphic type validation", invoker)
        yield result).provideLayer(judgeLayer)
        recorded <- calls.get
      yield
        val detailArgs = recorded.collect { case ("get_javadoc_symbol", args) => args }
        assertTrue(
          output.output.get("evidence").flatMap(_.asArray).exists(_.size == 2),
          output.metrics.toolCalls == 4,
          output.metrics.toolTimeMs >= 0L,
          output.metrics.summedToolTimeMs >= 0L,
          recorded.map(_._1) == List(
            "get_latest_version", "list_javadoc_symbols", "get_javadoc_symbol", "get_javadoc_symbol"
          ),
          detailArgs.map(_.get("version").flatMap(_.asString)).toSet == Set(Some("2.22.2")),
          detailArgs.map(_.get("link").flatMap(_.asString)).toSet == Set(Some("ptv.html"), Some("basic.html")),
        )
    },
    test("depth budget stops unfinished recursive synthesis") {
      JavadocsWorkflowPlanner.synthesize(
        "goal", "topic", input, definitions, maxDepth = 2,
      ).provideLayer(judgeLayer).exit.map(exit => assertTrue(exit.isFailure))
    },
  )
