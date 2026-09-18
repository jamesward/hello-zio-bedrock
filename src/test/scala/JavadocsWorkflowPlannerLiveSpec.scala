import com.jamesward.zio_typesafe_ai.TypeSafeAI
import com.jamesward.ziohttp.mcp.client.McpClient
import zio.*
import zio.http.{Client as HttpClient}
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.test.TestAspect.*

object JavadocsWorkflowPlannerLiveSpec extends ZIOSpecDefault:
  import JavadocsWorkflowPlanner.*

  private val goal =
    "Summarize the public classes in the latest jackson-databind release that are related to polymorphic type validation."

  private val input = Json.Obj(
    "goal" -> Json.Str(goal),
    "groupId" -> Json.Str("com.fasterxml.jackson.core"),
    "artifactId" -> Json.Str("jackson-databind"),
    "topic" -> Json.Str("polymorphic type validation"),
  )

  private def program(strategy: Strategy) = ZIO.scoped:
    for
      mcp <- McpClient.connect("https://www.javadocs.dev/mcp")
      definitions <- mcp.listTools
      workflow <- JavadocsWorkflowPlanner.synthesize(
        goal,
        "polymorphic type validation",
        input,
        definitions,
        strategy = strategy,
      )
      _ <- ZIO.logInfo(s"Live $strategy recursive Jev workflow: ${workflow.toJson.toJson}")
    yield
      val tools = workflow.steps.collect:
        case Step.Call(_, tool, _)         => tool
        case Step.FanOut(_, _, tool, _, _) => tool
      assertTrue(
        tools.contains("get_latest_version"),
        tools.contains("list_javadoc_symbols"),
        tools.contains("get_javadoc_symbol"),
        workflow.steps.exists(_.isInstanceOf[Step.Filter]),
        workflow.steps.exists(_.isInstanceOf[Step.FanOut]),
      )

  def spec = suite("live recursive Jev workflow synthesis")(
    test("forward planning produces resolver/list/filter/fanout plan") {
      program(Strategy.Forward)
    },
    test("backward planning produces resolver/list/filter/fanout plan") {
      program(Strategy.Backward)
    },
  ).provideSomeShared[Scope](
    HttpClient.default,
    TypeSafeAI.Client.live >>> WorkflowJudge.jev,
  ) @@ ifEnvSet("TYPESAFE_API_KEY") @@ withLiveClock @@ withLiveSystem @@ timeout(90.seconds) @@ sequential
