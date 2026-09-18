package orchestration

import orchestration.model.*
import orchestration.runtime.*
import zio.*
import zio.json.ast.Json
import zio.test.*
import zio.test.TestAspect.*

object EventReadinessWorkflowSpec extends ZIOSpecDefault:

  private val workflow = Workflow(
    Vector(
      Step.Call(
        "weather",
        "operation_a",
        Expr.Obj(Vector(
          "place" -> Expr.Input(List("event", "location")),
          "day" -> Expr.Input(List("event", "day")),
        )),
      ),
      Step.Call(
        "facility",
        "operation_b",
        Expr.Obj(Vector(
          "facility_code" -> Expr.Input(List("event", "venueId")),
        )),
      ),
      Step.Construct(
        "readiness_context",
        Expr.Obj(Vector(
          "event" -> Expr.Input(List("event")),
          "weather" -> Expr.Ref("weather"),
          "facility" -> Expr.Ref("facility"),
        )),
      ),
      Step.Generate(
        "brief",
        "operation_c",
        Expr.Obj(Vector("context" -> Expr.Ref("readiness_context"))),
      ),
    ),
    Expr.Ref("brief"),
  )

  private val input = Json.Obj(
    "event" -> Json.Obj(
      "name" -> Json.Str("Community Picnic"),
      "location" -> Json.Str("North Park"),
      "day" -> Json.Str("tomorrow"),
      "venueId" -> Json.Str("venue-17"),
    )
  )

  def spec = suite("neutral event-readiness workflow")(
    test("independent calls run in parallel, join, then generate") {
      for
        started <- Ref.make(Set.empty[String])
        bothStarted <- Promise.make[Nothing, Unit]
        generatedInput <- Ref.make(Option.empty[Json.Obj])
        invoker = new OperationInvoker:
          def call(operation: String, arguments: Json.Obj): Task[Json] =
            for
              size <- started.modify: current =>
                val next = current + operation
                next.size -> next
              _ <- bothStarted.succeed(()).when(size == 2)
              _ <- bothStarted.await
            yield operation match
              case "operation_a" => Json.Obj(
                "precipitation" -> Json.Str("low"),
                "wind_speed" -> Json.Num(8),
              )
              case "operation_b" => Json.Obj(
                "open" -> Json.Bool(true),
                "capacity" -> Json.Num(250),
              )
              case other => throw IllegalArgumentException(s"unexpected operation: $other")
        generator = new GenerativeInvoker:
          def generate(operation: String, arguments: Json.Obj): Task[Json] =
            generatedInput.set(Some(arguments)) *>
              ZIO.succeed(Json.Obj(
                "recommendation" -> Json.Str("proceed"),
                "reason" -> Json.Str("Venue is open and weather risk is low."),
              ))
        report <- WorkflowRuntime.execute(
          workflow,
          input,
          invoker,
          generator,
          ExecutionPolicy(maxParallelism = 2),
        )
        observed <- generatedInput.get
      yield
        val context = observed.flatMap(_.get("context")).flatMap(_.asObject)
        assertTrue(
          report.output.asObject.flatMap(_.get("recommendation")).flatMap(_.asString).contains("proceed"),
          report.metrics.operationCalls == 2,
          report.metrics.generativeCalls == 1,
          context.flatMap(_.get("weather")).flatMap(_.asObject).flatMap(_.get("precipitation")).flatMap(_.asString).contains("low"),
          context.flatMap(_.get("facility")).flatMap(_.asObject).flatMap(_.get("open")).flatMap(_.asBoolean).contains(true),
        )
    } @@ timeout(5.seconds),
    test("unresolved references fail rather than deadlock") {
      val invalid = Workflow(
        Vector(Step.Construct("broken", Expr.Ref("missing"))),
        Expr.Ref("broken"),
      )
      WorkflowRuntime.execute(
        invalid,
        Json.Obj(),
        new OperationInvoker:
          def call(operation: String, arguments: Json.Obj): Task[Json] = ZIO.dieMessage("unexpected"),
      ).either.map:
        case Left(_: ExecutionError.UnresolvedSteps) => assertCompletes
        case other                                   => assertNever(s"expected UnresolvedSteps, got $other")
    },
  )
