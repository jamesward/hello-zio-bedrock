package orchestration

import zio.*
import zio.json.ast.Json

object model:
  enum Expr:
    case Input(path: List[String])
    case Ref(stepId: String, path: List[String] = Nil)
    case Item(path: List[String])
    case Literal(value: Json)
    case Obj(fields: Vector[(String, Expr)])
    case Arr(items: Vector[Expr])

  enum Step:
    def id: String
    case Call(id: String, operation: String, arguments: Expr.Obj)
    case Construct(id: String, value: Expr)
    case Generate(id: String, operation: String, arguments: Expr.Obj)
    case FanOut(id: String, source: Expr, operation: String, arguments: Expr.Obj)

  case class Workflow(steps: Vector[Step], result: Expr):
    def toJson: Json = Json.Obj(
      "steps" -> Json.Arr(steps.map(stepToJson)*),
      "result" -> exprToJson(result),
    )

  case class ExecutionPolicy(maxParallelism: Int = 4, fanOutLimit: Option[Int] = Some(6))

  case class ExecutionMetrics(
    operationCalls: Int,
    generativeCalls: Int,
    summedOperationTimeMs: Long,
    operationTimeMs: Long,
    generativeTimeMs: Long,
    executionTimeMs: Long,
  )

  case class ExecutionReport(output: Json, values: Map[String, Json], metrics: ExecutionMetrics)

  sealed trait ExecutionError extends Throwable
  object ExecutionError:
    case class DuplicateStepId(id: String) extends ExecutionError
    case class UnresolvedSteps(ids: List[String]) extends ExecutionError
    case class MissingInput(path: List[String]) extends ExecutionError
    case class MissingReference(stepId: String, path: List[String]) extends ExecutionError
    case class MissingItem(path: List[String]) extends ExecutionError
    case class ExpectedObject(value: Json) extends ExecutionError
    case class ExpectedArray(value: Json) extends ExecutionError
    case class InvalidPolicy(message: String) extends ExecutionError

  private def exprToJson(expr: Expr): Json = expr match
    case Expr.Input(path) => Json.Obj("input" -> Json.Arr(path.map(Json.Str(_))*))
    case Expr.Ref(id, path) => Json.Obj("ref" -> Json.Str(id), "path" -> Json.Arr(path.map(Json.Str(_))*))
    case Expr.Item(path) => Json.Obj("item" -> Json.Arr(path.map(Json.Str(_))*))
    case Expr.Literal(value) => Json.Obj("literal" -> value)
    case Expr.Obj(fields) => Json.Obj("object" -> Json.Obj(fields.map((name, value) => name -> exprToJson(value))*))
    case Expr.Arr(items) => Json.Obj("array" -> Json.Arr(items.map(exprToJson)*))

  private def stepToJson(step: Step): Json = step match
    case Step.Call(id, operation, arguments) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("call"), "operation" -> Json.Str(operation), "arguments" -> exprToJson(arguments)
    )
    case Step.Construct(id, value) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("construct"), "value" -> exprToJson(value)
    )
    case Step.Generate(id, operation, arguments) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("generate"), "operation" -> Json.Str(operation), "arguments" -> exprToJson(arguments)
    )
    case Step.FanOut(id, source, operation, arguments) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("fan_out"), "source" -> exprToJson(source),
      "operation" -> Json.Str(operation), "arguments" -> exprToJson(arguments)
    )

object runtime:
  import model.*
  import model.ExecutionError.*

  trait OperationInvoker:
    def call(operation: String, arguments: Json.Obj): Task[Json]

  trait GenerativeInvoker:
    def generate(operation: String, arguments: Json.Obj): Task[Json]

  object GenerativeInvoker:
    val rejecting: GenerativeInvoker = new GenerativeInvoker:
      def generate(operation: String, arguments: Json.Obj): Task[Json] =
        ZIO.fail(IllegalStateException(s"No generative operation registered for '$operation'"))

  object WorkflowRuntime:
    private case class Timing(calls: Int, summedMs: Long, wallMs: Long)

    def execute(
      workflow: Workflow,
      input: Json,
      operations: OperationInvoker,
      generative: GenerativeInvoker = GenerativeInvoker.rejecting,
      policy: ExecutionPolicy = ExecutionPolicy(),
    ): IO[Throwable, ExecutionReport] =
      if policy.maxParallelism <= 0 then ZIO.fail(InvalidPolicy("maxParallelism must be positive"))
      else if policy.fanOutLimit.exists(_ <= 0) then ZIO.fail(InvalidPolicy("fanOutLimit must be positive when present"))
      else
        val ids = workflow.steps.map(_.id)
        ids.groupBy(identity).collectFirst { case (id, occurrences) if occurrences.size > 1 => id } match
          case Some(id) => ZIO.fail(DuplicateStepId(id))
          case None =>
            for
              started <- Clock.nanoTime
              operationTiming <- Ref.make(Timing(0, 0L, 0L))
              generativeTiming <- Ref.make(Timing(0, 0L, 0L))
              values <- executeWaves(
                workflow.steps,
                input,
                Map.empty,
                operations,
                generative,
                policy,
                operationTiming,
                generativeTiming,
              )
              output <- eval(workflow.result, input, values, None)
              finished <- Clock.nanoTime
              operation <- operationTiming.get
              generation <- generativeTiming.get
            yield ExecutionReport(
              output,
              values,
              ExecutionMetrics(
                operation.calls,
                generation.calls,
                operation.summedMs,
                operation.wallMs,
                generation.wallMs,
                (finished - started) / 1000000L,
              ),
            )

    private def executeWaves(
      pending: Vector[Step],
      input: Json,
      values: Map[String, Json],
      operations: OperationInvoker,
      generative: GenerativeInvoker,
      policy: ExecutionPolicy,
      operationTiming: Ref[Timing],
      generativeTiming: Ref[Timing],
    ): IO[Throwable, Map[String, Json]] =
      if pending.isEmpty then ZIO.succeed(values)
      else
        val (ready, blocked) = pending.partition(step => dependencies(step).subsetOf(values.keySet))
        if ready.isEmpty then ZIO.fail(UnresolvedSteps(blocked.map(_.id).toList))
        else
          ZIO.foreachPar(ready): step =>
            executeStep(step, input, values, operations, generative, policy, operationTiming, generativeTiming)
              .map(step.id -> _)
          .withParallelism(policy.maxParallelism)
          .flatMap: completed =>
            executeWaves(
              blocked,
              input,
              values ++ completed,
              operations,
              generative,
              policy,
              operationTiming,
              generativeTiming,
            )

    private def executeStep(
      step: Step,
      input: Json,
      values: Map[String, Json],
      operations: OperationInvoker,
      generative: GenerativeInvoker,
      policy: ExecutionPolicy,
      operationTiming: Ref[Timing],
      generativeTiming: Ref[Timing],
    ): IO[Throwable, Json] = step match
      case Step.Call(_, operation, arguments) =>
        for
          obj <- evalObject(arguments, input, values, None)
          result <- timedCall(operations, operation, obj, operationTiming, includeWall = true)
        yield result
      case Step.Construct(_, value) => eval(value, input, values, None)
      case Step.Generate(_, operation, arguments) =>
        for
          obj <- evalObject(arguments, input, values, None)
          started <- Clock.nanoTime
          result <- generative.generate(operation, obj)
          finished <- Clock.nanoTime
          elapsed = (finished - started) / 1000000L
          _ <- generativeTiming.update(timing => Timing(timing.calls + 1, timing.summedMs + elapsed, timing.wallMs + elapsed))
        yield result
      case Step.FanOut(_, source, operation, arguments) =>
        for
          sourceValue <- eval(source, input, values, None)
          items <- ZIO.fromOption(sourceValue.asArray).orElseFail(ExpectedArray(sourceValue))
          selected = policy.fanOutLimit.fold(items)(items.take)
          groupStarted <- Clock.nanoTime
          results <- ZIO.foreachPar(selected): item =>
            for
              obj <- evalObject(arguments, input, values, Some(item))
              result <- timedCall(operations, operation, obj, operationTiming, includeWall = false)
            yield result
          .withParallelism(policy.maxParallelism)
          groupFinished <- Clock.nanoTime
          _ <- operationTiming.update(timing => timing.copy(wallMs = timing.wallMs + (groupFinished - groupStarted) / 1000000L))
        yield Json.Arr(results)

    private def timedCall(
      operations: OperationInvoker,
      operation: String,
      arguments: Json.Obj,
      timingRef: Ref[Timing],
      includeWall: Boolean,
    ): Task[Json] =
      for
        started <- Clock.nanoTime
        result <- operations.call(operation, arguments)
        finished <- Clock.nanoTime
        elapsed = (finished - started) / 1000000L
        _ <- timingRef.update: timing =>
          Timing(
            timing.calls + 1,
            timing.summedMs + elapsed,
            timing.wallMs + (if includeWall then elapsed else 0L),
          )
      yield result

    private def dependencies(step: Step): Set[String] = step match
      case Step.Call(_, _, arguments)          => dependencies(arguments)
      case Step.Construct(_, value)            => dependencies(value)
      case Step.Generate(_, _, arguments)      => dependencies(arguments)
      case Step.FanOut(_, source, _, arguments) => dependencies(source) ++ dependencies(arguments)

    private def dependencies(expr: Expr): Set[String] = expr match
      case Expr.Input(_)       => Set.empty
      case Expr.Ref(id, _)     => Set(id)
      case Expr.Item(_)        => Set.empty
      case Expr.Literal(_)     => Set.empty
      case Expr.Obj(fields)    => fields.iterator.flatMap((_, value) => dependencies(value)).toSet
      case Expr.Arr(items)     => items.iterator.flatMap(dependencies).toSet

    private def evalObject(
      expression: Expr.Obj,
      input: Json,
      values: Map[String, Json],
      item: Option[Json],
    ): IO[ExecutionError, Json.Obj] =
      eval(expression, input, values, item).flatMap(json => ZIO.fromOption(json.asObject).orElseFail(ExpectedObject(json)))

    private def eval(
      expr: Expr,
      input: Json,
      values: Map[String, Json],
      item: Option[Json],
    ): IO[ExecutionError, Json] = expr match
      case Expr.Input(path) => descend(input, path).mapError(_ => MissingInput(path))
      case Expr.Ref(stepId, path) =>
        ZIO.fromOption(values.get(stepId)).orElseFail(MissingReference(stepId, path))
          .flatMap(value => descend(value, path).mapError(_ => MissingReference(stepId, path)))
      case Expr.Item(path) =>
        ZIO.fromOption(item).orElseFail(MissingItem(path))
          .flatMap(value => descend(value, path).mapError(_ => MissingItem(path)))
      case Expr.Literal(value) => ZIO.succeed(value)
      case Expr.Obj(fields) =>
        ZIO.foreach(fields) { case (name, value) => eval(value, input, values, item).map(name -> _) }
          .map(entries => Json.Obj(Chunk.fromIterable(entries)))
      case Expr.Arr(items) =>
        ZIO.foreach(items)(eval(_, input, values, item)).map(values => Json.Arr(Chunk.fromIterable(values)))

    private def descend(value: Json, path: List[String]): IO[Unit, Json] =
      path.foldLeft[IO[Unit, Json]](ZIO.succeed(value)): (current, field) =>
        current.flatMap(json => ZIO.fromOption(json.asObject.flatMap(_.get(field))).orElseFail(()))
