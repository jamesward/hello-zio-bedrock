import com.jamesward.zio_typesafe_ai.TypeSafeAI
import com.jamesward.zio_typesafe_ai.TypeSafeAI.*
import com.jamesward.ziohttp.mcp.{CallToolResult, ToolContent, ToolDefinition}
import com.jamesward.ziohttp.mcp.client.McpClient
import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.schema.codec.json.schemaJson

object JavadocsWorkflowPlanner:

  enum Shape:
    case Str
    case Bool
    case Num
    case Obj(properties: Map[String, Shape])
    case Arr(items: Shape)
    case Unknown

  enum Expr:
    case Input(field: String)
    case Ref(stepId: String, path: List[String] = Nil)
    case Item(path: List[String])

  enum Step:
    case Call(id: String, tool: String, arguments: Map[String, Expr])
    case Filter(id: String, source: Expr, topic: String, inspectLimit: Int = 40, keepLimit: Int = 6)
    case FanOut(id: String, source: Expr, tool: String, arguments: Map[String, Expr], limit: Int = 6)

  case class Workflow(steps: Vector[Step], result: Expr):
    def toJson: Json = Json.Obj(
      "steps" -> Json.Arr(steps.map(stepJson)*),
      "result" -> exprJson(result),
    )

  enum Strategy:
    case Forward
    case Backward

  case class Candidate(id: String, key: String, view: Json.Obj, transition: Transition)

  enum Transition:
    case AddCall(step: Step.Call, output: Shape, producer: String)
    case AddFilter(step: Step.Filter, output: Shape)
    case AddFanOut(step: Step.FanOut, output: Shape)
    case Finish(result: Expr)

  case class JevUsage(turns: Int, inputTokens: Int, outputTokens: Int, timeMs: Long):
    def totalTokens: Int = inputTokens + outputTokens
    def add(usage: TypeSafeAI.Usage, elapsedMs: Long): JevUsage =
      JevUsage(turns + 1, inputTokens + usage.inputTokens, outputTokens + usage.outputTokens, timeMs + elapsedMs)

  object JevUsage:
    val empty: JevUsage = JevUsage(0, 0, 0, 0L)

  trait WorkflowJudge:
    def choose(goal: String, partial: Json, candidates: NonEmptyChunk[Candidate]): Task[String]
    def relevant(goal: String, items: Chunk[Json], keepLimit: Int): Task[Chunk[Int]]
    def usage: UIO[JevUsage]

  object WorkflowJudge:
    val jev: URLayer[TypeSafeAI.Client, WorkflowJudge] =
      ZLayer.fromZIO:
        for
          client <- ZIO.service[TypeSafeAI.Client]
          usage <- Ref.make(JevUsage.empty)
        yield JevJudge(client, usage)

  private final case class ToolSpec(
    name: String,
    description: String,
    inputSchema: Json.Obj,
    outputSchema: Json.Obj,
    required: List[String],
    inputProperties: Map[String, Shape],
    output: Shape,
  )

  private final case class Available(
    expr: Expr,
    shape: Shape,
    filtered: Boolean,
    producer: Option[String],
  )

  private final case class State(
    goal: String,
    topic: String,
    tools: List[ToolSpec],
    available: Vector[Available],
    steps: Vector[Step],
    usedCalls: Set[String],
    depth: Int,
  )

  private enum BackwardRequirement:
    case DetailEvidence
    case CollectionProducer(detail: ToolSpec)
    case ScalarPrerequisite(detail: ToolSpec, collection: ToolSpec)

  private final case class JevJudge(client: TypeSafeAI.Client, usageRef: Ref[JevUsage]) extends WorkflowJudge:
    def usage: UIO[JevUsage] = usageRef.get

    private def record(usage: TypeSafeAI.Usage, elapsedMs: Long): UIO[Unit] =
      usageRef.update(_.add(usage, elapsedMs))

    def choose(goal: String, partial: Json, candidates: NonEmptyChunk[Candidate]): Task[String] =
      val options = candidates.map(candidate => candidate.id -> (Content[Json](candidate.view): Content | Null)).toMap
      for
        criteria <- ZIO.fromEither(ChoiceCriteria.fromContent(options)).mapError(IllegalArgumentException(_))
        state: Json = Json.Obj(
          "goal" -> Json.Str(goal),
          "partialWorkflow" -> partial,
          "candidateCount" -> Json.Num(candidates.size),
        )
        request <- ZIO.fromEither(TypeSafeAI.askDynamic(
          state,
          List(QuestionId("next_action") -> Question.Choice(
            "Choose the single candidate transition that best advances the goal toward a complete evidence-producing workflow. Candidate ids are opaque; judge the structured action, tool semantics, bindings, and output schema. Choose Finish only when the workflow already produces sufficient detailed evidence.",
            criteria,
          )),
        )).mapError(IllegalArgumentException(_))
        started <- Clock.nanoTime
        result <- request.run.provideEnvironment(ZEnvironment(client))
        finished <- Clock.nanoTime
        _ <- record(result.usage, (finished - started) / 1000000L)
        answer <- ZIO.fromOption(result.answers.get(QuestionId("next_action")))
          .orElseFail(IllegalStateException("Jev omitted next_action"))
        selected <- answer match
          case DynamicAnswer.Choice(value) => ZIO.succeed(value.choice)
          case other => ZIO.fail(IllegalStateException(s"Expected Choice answer, got $other"))
        _ <- ZIO.fail(IllegalStateException(s"Jev selected unknown candidate '$selected'"))
          .unless(candidates.exists(_.id == selected))
      yield selected

    def relevant(goal: String, items: Chunk[Json], keepLimit: Int): Task[Chunk[Int]] =
      if items.isEmpty then ZIO.succeed(Chunk.empty)
      else
        val maxBatchChars = 30000
        val indexed = items.zipWithIndex
        val batches = indexed.foldLeft(Vector.empty[Vector[(Json, Int)]]):
          case (Vector(), entry) => Vector(Vector(entry))
          case (acc, entry) =>
            val current = acc.last
            val currentChars = current.iterator.map(_._1.toJson.length).sum
            if currentChars + entry._1.toJson.length <= maxBatchChars then acc.init :+ (current :+ entry)
            else acc :+ Vector(entry)

        ZIO.foreachPar(batches): batch =>
          val batchItems = Chunk.fromIterable(batch.map(_._1))
          val questions = batch.zipWithIndex.map { case ((_, globalIndex), localIndex) =>
            QuestionId(s"item_$localIndex") -> Question.Noul[Json](
              Json.Obj(
                "instruction" -> Json.Str("Is the candidate directly and specifically about validating polymorphic types or subtype safety? Exclude generic serialization, deserialization, naming, node, property, type-resolution, or utility APIs that are not themselves validators or validator support types."),
                "candidateIndex" -> Json.Num(localIndex),
              ),
              null,
            )
          }
          val state: Json = Json.Obj("goal" -> Json.Str(goal), "candidates" -> Json.Arr(batchItems*))
          for
            request <- ZIO.fromEither(TypeSafeAI.askDynamic(state, questions)).mapError(IllegalArgumentException(_))
            started <- Clock.nanoTime
            result <- request.run.provideEnvironment(ZEnvironment(client))
            finished <- Clock.nanoTime
            _ <- record(result.usage, (finished - started) / 1000000L)
            scored <- ZIO.foreach(batch.zipWithIndex) { case ((_, globalIndex), localIndex) =>
              result.answers.get(QuestionId(s"item_$localIndex")) match
                case Some(DynamicAnswer.Noul(probability)) => ZIO.succeed(globalIndex -> probability.unwrap)
                case Some(other) => ZIO.fail(IllegalStateException(s"Expected Noul relevance answer, got $other"))
                case None => ZIO.fail(IllegalStateException(s"Missing relevance answer item_$localIndex"))
            }
          yield scored
        .withParallelism(4).map: scoredBatches =>
          val ordered = scoredBatches.flatten.sortBy { case (_, probability) => -probability }
          val selected =
            if keepLimit == Int.MaxValue then
              if ordered.size <= 1 then ordered
              else
                val gaps = ordered.zip(ordered.drop(1)).zipWithIndex.map:
                  case (((_, high), (_, low)), index) => (high - low, index)
                val splitAfter = gaps.maxBy(_._1)._2 + 1
                ordered.take(splitAfter)
            else ordered.filter(_._2 >= 0.5).take(keepLimit)
          val fallback = if selected.nonEmpty then selected else ordered.take(if keepLimit == Int.MaxValue then 1 else math.min(2, keepLimit))
          Chunk.fromIterable(fallback.map(_._1))


  def synthesize(
    goal: String,
    topic: String,
    input: Json.Obj,
    definitions: Chunk[ToolDefinition],
    maxDepth: Int = 7,
    strategy: Strategy = Strategy.Forward,
  ): ZIO[WorkflowJudge, Throwable, Workflow] =
    for
      tools <- ZIO.foreach(definitions)(toolSpec)
      workflow <- strategy match
        case Strategy.Forward =>
          val initialValues = input.fields.collect:
            case (name, Json.Str(_))  => Available(Expr.Input(name), Shape.Str, filtered = false, None)
            case (name, Json.Bool(_)) => Available(Expr.Input(name), Shape.Bool, filtered = false, None)
            case (name, Json.Num(_))  => Available(Expr.Input(name), Shape.Num, filtered = false, None)
          val initial = State(goal, topic, tools.toList, initialValues.toVector, Vector.empty, Set.empty, 0)
          loop(initial, maxDepth)
        case Strategy.Backward => backward(goal, topic, input, tools.toList)
    yield workflow

  private def loop(state: State, maxDepth: Int): ZIO[WorkflowJudge, Throwable, Workflow] =
    if state.depth >= maxDepth then ZIO.fail(IllegalStateException(s"Workflow synthesis exceeded max depth $maxDepth"))
    else
      val generated = candidates(state).take(64).zipWithIndex.map: (candidate, index) =>
        candidate.copy(id = f"c$index%03d", view = Json.Obj(candidate.view.fields :+ ("candidateId" -> Json.Str(f"c$index%03d"))))
      NonEmptyChunk.fromIterableOption(generated) match
        case None => ZIO.fail(IllegalStateException(s"No valid workflow transitions at depth ${state.depth}"))
        case Some(nonEmpty) =>
          for
            selectedId <- ZIO.serviceWithZIO[WorkflowJudge](_.choose(state.goal, renderPartial(state), nonEmpty))
            selected <- ZIO.fromOption(generated.find(_.id == selectedId))
              .orElseFail(IllegalStateException(s"Unknown selected candidate '$selectedId'"))
            _ <- ZIO.logInfo(
              s"[jev-planner:forward] depth=${state.depth} selected=${selected.key} candidates=${generated.map(_.key).mkString(",")}" 
            )
            result <- selected.transition match
              case Transition.Finish(expr) => ZIO.succeed(Workflow(state.steps, expr))
              case transition => loop(applyTransition(state, transition), maxDepth)
          yield result

  def synthesizeWithTypeSafeLoop(
    goal: String,
    topic: String,
    input: Json.Obj,
    definitions: Chunk[ToolDefinition],
    maxDepth: Int = 7,
  ): ZIO[TypeSafeAI.Client, Throwable, TypeSafeAI.LoopResult[Workflow]] =
    for
      tools <- ZIO.foreach(definitions)(toolSpec)
      initialValues = input.fields.collect:
        case (name, Json.Str(_))  => Available(Expr.Input(name), Shape.Str, filtered = false, None)
        case (name, Json.Bool(_)) => Available(Expr.Input(name), Shape.Bool, filtered = false, None)
        case (name, Json.Num(_))  => Available(Expr.Input(name), Shape.Num, filtered = false, None)
      initial = State(goal, topic, tools.toList, initialValues.toVector, Vector.empty, Set.empty, 0)
      result <- TypeSafeAI.loop[State, Candidate, Any, Throwable, Workflow](initial)(
        state => Content[Json](Json.Obj(
          "goal" -> Json.Str(state.goal),
          "partialWorkflow" -> renderPartial(state),
        )),
        state =>
          val generated = candidates(state).take(64).zipWithIndex.map: (candidate, index) =>
            val id = f"c$index%03d"
            candidate.copy(id = id, view = Json.Obj(candidate.view.fields :+ ("candidateId" -> Json.Str(id))))
          ZIO.fromOption(NonEmptyChunk.fromIterableOption(generated))
            .orElseFail(IllegalStateException(s"No valid workflow transitions at depth ${state.depth}"))
            .map(_.map(candidate => TypeSafeAI.LoopOption(
              candidate.id,
              candidate,
              Content[Json](candidate.view),
            )))
      ) { (state, selected) =>
        selected.transition match
          case Transition.Finish(expr) =>
            ZIO.succeed(TypeSafeAI.LoopStep.Done(Workflow(state.steps, expr)))
          case transition =>
            ZIO.succeed(TypeSafeAI.LoopStep.Continue(applyTransition(state, transition)))
      }.maxIterations(maxDepth).run
    yield result

  private def backward(
    goal: String,
    topic: String,
    input: Json.Obj,
    tools: List[ToolSpec],
  ): ZIO[WorkflowJudge, Throwable, Workflow] =
    def select(requirement: String, options: List[ToolSpec], selected: List[String]): ZIO[WorkflowJudge, Throwable, ToolSpec] =
      val generated = options.sortBy(_.name).zipWithIndex.map: (tool, index) =>
        val id = f"b$index%03d"
        Candidate(
          id,
          s"backward:$requirement:${tool.name}",
          Json.Obj(
            "candidateId" -> Json.Str(id),
            "planningDirection" -> Json.Str("backward"),
            "requirement" -> Json.Str(requirement),
            "tool" -> Json.Str(tool.name),
            "description" -> Json.Str(tool.description),
            "inputSchema" -> tool.inputSchema,
            "outputSchema" -> tool.outputSchema,
            "alreadySelectedConsumers" -> Json.Arr(selected.map(Json.Str(_))*),
          ),
          Transition.Finish(Expr.Input("goal")),
        )
      NonEmptyChunk.fromIterableOption(generated) match
        case None => ZIO.fail(IllegalStateException(s"No backward candidates for $requirement"))
        case Some(candidates) =>
          for
            chosenId <- ZIO.serviceWithZIO[WorkflowJudge](_.choose(
              goal,
              Json.Obj("direction" -> Json.Str("backward"), "requirement" -> Json.Str(requirement)),
              candidates,
            ))
            candidate <- ZIO.fromOption(generated.find(_.id == chosenId))
              .orElseFail(IllegalStateException(s"Unknown backward choice '$chosenId'"))
            toolName = candidate.key.split(":").last
            tool <- ZIO.fromOption(options.find(_.name == toolName))
              .orElseFail(IllegalStateException(s"Backward choice did not map to tool '$toolName'"))
            _ <- ZIO.logInfo(s"[jev-planner:backward] requirement=$requirement selected=${tool.name}")
          yield tool

    def resolve(requirement: BackwardRequirement): ZIO[WorkflowJudge, Throwable, Workflow] = requirement match
      case BackwardRequirement.DetailEvidence =>
        val options = tools.filter(tool =>
          tool.required.contains("link") && scalarLeaves(tool.output).exists(_._2 == Shape.Str)
        )
        select("rendered public API documentation for named classes; implementation source is not desired", options, Nil)
          .flatMap(detail => resolve(BackwardRequirement.CollectionProducer(detail)))

      case BackwardRequirement.CollectionProducer(detail) =>
        val itemRequirements = detail.required.filterNot(field => input.get(field).nonEmpty || field == "version")
        val options = tools.filter(tool => nestedArrays(tool.output).exists:
          case (_, Shape.Arr(Shape.Obj(properties))) =>
            itemRequirements.nonEmpty && itemRequirements.forall(properties.contains)
          case _ => false
        )
        select("collection whose item fields satisfy the detail tool inputs", options, List(detail.name))
          .flatMap(collection => resolve(BackwardRequirement.ScalarPrerequisite(detail, collection)))

      case BackwardRequirement.ScalarPrerequisite(detail, collection) =>
        val itemRequirements = detail.required.filterNot(field => input.get(field).nonEmpty || field == "version")
        val missing = collection.required.filterNot(input.get(_).nonEmpty)
        val options = tools.filter: tool =>
          tool.required.forall(input.get(_).nonEmpty) &&
          scalarLeaves(tool.output).exists(_._2 == Shape.Str) &&
          missing.exists(field => field == "version" && (tool.name + tool.description).toLowerCase.contains("version"))
        select("scalar prerequisite required by the collection tool", options, List(detail.name, collection.name)).map: versionTool =>
          val versionPath = scalarLeaves(versionTool.output).find(_._1.lastOption.contains("result"))
            .orElse(scalarLeaves(versionTool.output).headOption).map(_._1).getOrElse(Nil)
          val collectionPath = nestedArrays(collection.output).collectFirst:
            case (path, Shape.Arr(Shape.Obj(properties))) if itemRequirements.forall(properties.contains) => path
          .getOrElse(throw IllegalStateException("Selected collection has no compatible item array"))
          val versionArgs = versionTool.required.map(field => field -> Expr.Input(field)).toMap
          val collectionArgs = collection.required.map: field =>
            field -> (if field == "version" then Expr.Ref("n1", versionPath) else Expr.Input(field))
          .toMap
          val detailArgs = detail.required.map: field =>
            field -> (if field == "link" then Expr.Item(List("link"))
              else if field == "version" then Expr.Ref("n1", versionPath)
              else Expr.Input(field))
          .toMap
          Workflow(
            Vector(
              Step.Call("n1", versionTool.name, versionArgs),
              Step.Call("n2", collection.name, collectionArgs),
              Step.Filter("n3", Expr.Ref("n2", collectionPath), topic),
              Step.FanOut("n4", Expr.Ref("n3"), detail.name, detailArgs),
            ),
            Expr.Ref("n4"),
          )

    resolve(BackwardRequirement.DetailEvidence)

  private def candidates(state: State): List[Candidate] =
    val nextId = s"n${state.steps.size + 1}"
    val calls = state.tools
      .filterNot(tool => state.usedCalls.contains(tool.name))
      .flatMap: tool =>
        bindArguments(tool.required, state.available, item = None).map: arguments =>
          val step: Step.Call = Step.Call(nextId, tool.name, arguments)
          candidate(
            s"call:${tool.name}",
            Json.Obj(
              "action" -> Json.Str("call"),
              "tool" -> Json.Str(tool.name),
              "description" -> Json.Str(tool.description),
              "arguments" -> argsJson(arguments),
              "inputSchema" -> tool.inputSchema,
              "outputSchema" -> tool.outputSchema,
            ),
            Transition.AddCall(step, tool.output, tool.name),
          )

    val filters = state.available.collect:
      case available @ Available(_, Shape.Arr(Shape.Obj(properties)), false, producer)
          if properties.contains("link") && !state.steps.exists:
            case Step.Filter(_, source, _, _, _) => source == available.expr
            case _                              => false
          =>
        val step: Step.Filter = Step.Filter(nextId, available.expr, state.topic)
        candidate(
          s"filter:${producer.getOrElse("records")}",
          Json.Obj(
            "action" -> Json.Str("semantic_filter"),
            "source" -> exprJson(available.expr),
            "topic" -> Json.Str(state.topic),
            "itemFields" -> Json.Arr(properties.keys.toList.sorted.map(Json.Str(_))*),
            "purpose" -> Json.Str("Reduce fan-out by retaining only records semantically relevant to the goal."),
          ),
          Transition.AddFilter(step, available.shape),
        )

    val fanOuts = state.available.collect {
      case available @ Available(_, Shape.Arr(item @ Shape.Obj(_)), true, _) =>
        state.tools.filterNot(tool => state.usedCalls.contains(tool.name)).flatMap: tool =>
          bindArguments(tool.required, state.available, item = Some(item)).filter(arguments => arguments.values.exists:
            case Expr.Item(_) => true
            case _            => false
          ).map: arguments =>
            val step: Step.FanOut = Step.FanOut(nextId, available.expr, tool.name, arguments)
            candidate(
              s"fanout:${tool.name}",
              Json.Obj(
                "action" -> Json.Str("bounded_fan_out"),
                "tool" -> Json.Str(tool.name),
                "description" -> Json.Str(tool.description),
                "source" -> exprJson(available.expr),
                "arguments" -> argsJson(arguments),
                "outputSchema" -> tool.outputSchema,
                "limit" -> Json.Num(6),
              ),
              Transition.AddFanOut(step, Shape.Arr(tool.output)),
            )
    }.flatten

    val finishes = state.steps.lastOption.toList.collect:
      case Step.FanOut(id, _, _, _, _) =>
        candidate(
          "finish",
          Json.Obj(
            "action" -> Json.Str("finish"),
            "result" -> exprJson(Expr.Ref(id)),
            "purpose" -> Json.Str("Return the collected detailed evidence to the outer model."),
          ),
          Transition.Finish(Expr.Ref(id)),
        )

    (calls ++ filters ++ fanOuts ++ finishes).sortBy(_.key)

  private def candidate(key: String, view: Json.Obj, transition: Transition): Candidate =
    Candidate("", key, view, transition)

  private def applyTransition(state: State, transition: Transition): State = transition match
    case Transition.AddCall(step, output, producer) =>
      val root = Available(Expr.Ref(step.id), output, filtered = false, Some(producer))
      val nested = nestedArrays(output).map: (path, shape) =>
        Available(Expr.Ref(step.id, path), shape, filtered = false, Some(producer))
      state.copy(
        available = state.available ++ (root +: nested),
        steps = state.steps :+ step,
        usedCalls = state.usedCalls + producer,
        depth = state.depth + 1,
      )
    case Transition.AddFilter(step, output) =>
      state.copy(
        available = state.available :+ Available(Expr.Ref(step.id), output, filtered = true, Some("semantic_filter")),
        steps = state.steps :+ step,
        depth = state.depth + 1,
      )
    case Transition.AddFanOut(step, output) =>
      state.copy(
        available = state.available :+ Available(Expr.Ref(step.id), output, filtered = false, Some(step.tool)),
        steps = state.steps :+ step,
        usedCalls = state.usedCalls + step.tool,
        depth = state.depth + 1,
      )
    case Transition.Finish(_) => state

  private def bindArguments(
    required: List[String],
    available: Vector[Available],
    item: Option[Shape.Obj],
  ): Option[Map[String, Expr]] =
    required.foldLeft(Option(Map.empty[String, Expr])): (acc, field) =>
      for
        soFar <- acc
        binding <- bestBinding(field, available, item)
      yield soFar.updated(field, binding)

  private def bestBinding(field: String, available: Vector[Available], item: Option[Shape.Obj]): Option[Expr] =
    val itemBinding = item.flatMap(_.properties.get(field)).collect:
      case shape if scalar(shape) => Expr.Item(List(field))
    itemBinding.orElse:
      val leaves = available.zipWithIndex.flatMap: (value, index) =>
        scalarLeaves(value.shape).map: (path, shape) =>
          val terminal = path.lastOption
          val inputExact =
            if path.isEmpty then
              value.expr match
                case Expr.Input(name) => name == field
                case _                => false
            else false
          val exact = terminal.contains(field) || inputExact
          val semantic =
            field == "version" && value.producer.exists(_.contains("latest_version")) && terminal.contains("result")
          val score = if exact then 0 else if semantic then 1 else 100
          val expr = if path.isEmpty then value.expr else value.expr match
            case Expr.Ref(stepId, prefix) => Expr.Ref(stepId, prefix ++ path)
            case Expr.Input(name) if path.isEmpty => Expr.Input(name)
            case other => other
          (score, -index, expr)
      leaves.filter(_._1 < 100).sortBy(entry => (entry._1, entry._2)).headOption.map(_._3)

  private def scalarLeaves(shape: Shape, prefix: List[String] = Nil): List[(List[String], Shape)] = shape match
    case scalarShape if scalar(scalarShape) => List(prefix -> scalarShape)
    case Shape.Obj(properties) => properties.toList.flatMap((name, child) => scalarLeaves(child, prefix :+ name))
    case _ => Nil

  private def nestedArrays(shape: Shape, prefix: List[String] = Nil): List[(List[String], Shape.Arr)] = shape match
    case array: Shape.Arr => List(prefix -> array)
    case Shape.Obj(properties) => properties.toList.flatMap((name, child) => nestedArrays(child, prefix :+ name))
    case _ => Nil

  private def scalar(shape: Shape): Boolean = shape match
    case Shape.Str | Shape.Bool | Shape.Num => true
    case _                                  => false

  private def toolSpec(definition: ToolDefinition): Task[ToolSpec] =
    val name = definition.name.value
    val inputShape = parseShape(definition.inputSchema)
    val inputProperties = inputShape match
      case Shape.Obj(properties) => properties
      case _ => Map.empty
    val required = definition.inputSchema.get("required").flatMap(_.asArray)
      .map(_.flatMap(_.asString).toList).getOrElse(inputProperties.keys.toList)
    val effectiveOutput = definition.outputSchema.getOrElse(Json.Obj(
      "type" -> Json.Str("object"),
      "properties" -> Json.Obj("result" -> Json.Obj("type" -> Json.Str("string"))),
      "required" -> Json.Arr(Json.Str("result")),
    ))
    ZIO.succeed(ToolSpec(
      name,
      definition.description.getOrElse(name),
      definition.inputSchema,
      effectiveOutput,
      required,
      inputProperties,
      parseShape(effectiveOutput),
    ))

  private def parseShape(schema: Json): Shape =
    val obj = schema.asObject
    val typeNames = obj.flatMap(_.get("type")) match
      case Some(Json.Str(value)) => Set(value)
      case Some(Json.Arr(values)) => values.flatMap(_.asString).toSet - "null"
      case _ => Set.empty[String]
    if typeNames.contains("object") || obj.exists(_.get("properties").nonEmpty) then
      val properties = obj.flatMap(_.get("properties")).flatMap(_.asObject)
        .map(_.fields.map((name, value) => name -> parseShape(value)).toMap).getOrElse(Map.empty)
      Shape.Obj(properties)
    else if typeNames.contains("array") then
      Shape.Arr(obj.flatMap(_.get("items")).map(parseShape).getOrElse(Shape.Unknown))
    else if typeNames.contains("string") then Shape.Str
    else if typeNames.contains("boolean") then Shape.Bool
    else if typeNames.contains("integer") || typeNames.contains("number") then Shape.Num
    else Shape.Unknown

  private def renderPartial(state: State): Json = Json.Obj(
    "depth" -> Json.Num(state.depth),
    "steps" -> Json.Arr(state.steps.map(stepJson)*),
    "availableValues" -> Json.Arr(state.available.map: value =>
      Json.Obj(
        "reference" -> exprJson(value.expr),
        "shape" -> shapeJson(value.shape),
        "filtered" -> Json.Bool(value.filtered),
        "producer" -> value.producer.fold[Json](Json.Null)(Json.Str(_)),
      )
    *),
  )

  private def stepJson(step: Step): Json = step match
    case Step.Call(id, tool, arguments) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("call"), "tool" -> Json.Str(tool), "arguments" -> argsJson(arguments)
    )
    case Step.Filter(id, source, topic, inspect, keep) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("semantic_filter"), "source" -> exprJson(source),
      "topic" -> Json.Str(topic), "inspectLimit" -> Json.Num(inspect), "keepLimit" -> Json.Num(keep)
    )
    case Step.FanOut(id, source, tool, arguments, limit) => Json.Obj(
      "id" -> Json.Str(id), "kind" -> Json.Str("fan_out"), "source" -> exprJson(source),
      "tool" -> Json.Str(tool), "arguments" -> argsJson(arguments), "limit" -> Json.Num(limit)
    )

  private def argsJson(arguments: Map[String, Expr]): Json.Obj =
    Json.Obj(arguments.toList.sortBy(_._1).map((name, expr) => name -> exprJson(expr))*)

  private def exprJson(expr: Expr): Json = expr match
    case Expr.Input(field) => Json.Obj("input" -> Json.Str(field))
    case Expr.Ref(stepId, path) => Json.Obj(
      "ref" -> Json.Str(stepId), "path" -> Json.Arr(path.map(Json.Str(_))*)
    )
    case Expr.Item(path) => Json.Obj("itemPath" -> Json.Arr(path.map(Json.Str(_))*))

  private def shapeJson(shape: Shape): Json = shape match
    case Shape.Str => Json.Str("string")
    case Shape.Bool => Json.Str("boolean")
    case Shape.Num => Json.Str("number")
    case Shape.Unknown => Json.Str("unknown")
    case Shape.Arr(items) => Json.Obj("array" -> shapeJson(items))
    case Shape.Obj(properties) => Json.Obj("object" -> Json.Obj(properties.toList.map((name, value) => name -> shapeJson(value))*))

object JavadocsWorkflowExecutor:
  import JavadocsWorkflowPlanner.*
  import orchestration.model.{ExecutionPolicy as NeutralPolicy, Expr as NeutralExpr, Step as NeutralStep, Workflow as NeutralWorkflow}
  import orchestration.runtime.{GenerativeInvoker, OperationInvoker, WorkflowRuntime}

  trait ToolInvoker:
    def call(name: String, arguments: Json.Obj): Task[CallToolResult]

  object ToolInvoker:
    def fromMcp(client: McpClient): ToolInvoker = new ToolInvoker:
      def call(name: String, arguments: Json.Obj): Task[CallToolResult] = client.callTool(name, arguments)

  case class ExecutionPolicy(unbounded: Boolean = false)
  case class ExecutionMetrics(toolCalls: Int, summedToolTimeMs: Long, toolTimeMs: Long)
  case class ExecutionResult(output: Json.Obj, metrics: ExecutionMetrics)

  def execute(
    workflow: Workflow,
    input: Json.Obj,
    goal: String,
    invoker: ToolInvoker,
    policy: ExecutionPolicy = ExecutionPolicy(),
  ): ZIO[WorkflowJudge, Throwable, ExecutionResult] =
    ZIO.serviceWithZIO[WorkflowJudge]: judge =>
      val operations = new OperationInvoker:
        def call(operation: String, arguments: Json.Obj): Task[Json] =
          normalizeCall(invoker, operation, arguments)

      val generative = new GenerativeInvoker:
        def generate(operation: String, arguments: Json.Obj): Task[Json] =
          if operation != "semantic_relevance_filter" then
            ZIO.fail(IllegalArgumentException(s"Unknown Javadocs generative operation '$operation'"))
          else
            for
              source <- ZIO.fromOption(arguments.get("source").flatMap(_.asArray))
                .orElseFail(IllegalArgumentException("semantic_relevance_filter requires array source"))
              semanticGoal = arguments.get("goal").flatMap(_.asString).getOrElse(goal)
              topic = arguments.get("topic").flatMap(_.asString).getOrElse("")
              inspectLimit = arguments.get("inspectLimit").flatMap(_.asNumber).map(_.value.intValue).getOrElse(40)
              keepLimit = arguments.get("keepLimit").flatMap(_.asNumber).map(_.value.intValue).getOrElse(6)
              unbounded = arguments.get("unbounded").flatMap(_.asBoolean).contains(true)
              candidates = if unbounded then lexicalCandidates(source, s"$semanticGoal $topic")
                else lexicalPrefilter(source, s"$semanticGoal $topic", inspectLimit)
              effectiveKeep = if unbounded then Int.MaxValue else keepLimit
              selected <- judge.relevant(semanticGoal, candidates, effectiveKeep)
            yield Json.Arr(selected.flatMap(index => candidates.lift(index)))

      val neutral = compile(workflow, goal, policy)
      val neutralPolicy = NeutralPolicy(
        maxParallelism = 4,
        fanOutLimit = if policy.unbounded then None else Some(6),
      )
      WorkflowRuntime.execute(neutral, input, operations, generative, neutralPolicy).map: report =>
        val evidence = compact(report.output, policy)
        ExecutionResult(
          Json.Obj("workflow" -> workflow.toJson, "evidence" -> evidence),
          ExecutionMetrics(
            report.metrics.operationCalls,
            report.metrics.summedOperationTimeMs,
            report.metrics.operationTimeMs,
          ),
        )

  private def compile(workflow: Workflow, goal: String, policy: ExecutionPolicy): NeutralWorkflow =
    val steps = workflow.steps.map:
      case Step.Call(id, tool, arguments) =>
        NeutralStep.Call(id, tool, NeutralExpr.Obj(arguments.toVector.map((name, expr) => name -> convert(expr))))
      case Step.Filter(id, source, topic, inspectLimit, keepLimit) =>
        NeutralStep.Generate(
          id,
          "semantic_relevance_filter",
          NeutralExpr.Obj(Vector(
            "source" -> convert(source),
            "goal" -> NeutralExpr.Literal(Json.Str(goal)),
            "topic" -> NeutralExpr.Literal(Json.Str(topic)),
            "inspectLimit" -> NeutralExpr.Literal(Json.Num(inspectLimit)),
            "keepLimit" -> NeutralExpr.Literal(Json.Num(keepLimit)),
            "unbounded" -> NeutralExpr.Literal(Json.Bool(policy.unbounded)),
          )),
        )
      case Step.FanOut(id, source, tool, arguments, _) =>
        NeutralStep.FanOut(
          id,
          convert(source),
          tool,
          NeutralExpr.Obj(arguments.toVector.map((name, expr) => name -> convert(expr))),
        )
    NeutralWorkflow(steps, convert(workflow.result))

  private def convert(expr: Expr): NeutralExpr = expr match
    case Expr.Input(field)      => NeutralExpr.Input(List(field))
    case Expr.Ref(id, path)     => NeutralExpr.Ref(id, path)
    case Expr.Item(path)        => NeutralExpr.Item(path)

  private def normalizeCall(invoker: ToolInvoker, tool: String, arguments: Json.Obj): Task[Json] =
    for
      _ <- Console.printLine(s"[synthetic:mcp] -> $tool ${arguments.toJson}").orDie
      result <- invoker.call(tool, arguments)
      text = result.content.collect { case ToolContent.Text(value, _) => value }.mkString("\n")
      _ <- ZIO.fail(IllegalStateException(s"MCP tool $tool failed: $text")).when(result.isError.contains(true))
      normalized <- result.structuredContent match
        case Some(value: Json.Obj) => ZIO.succeed(value)
        case Some(value)           => ZIO.succeed(Json.Obj("result" -> value))
        case None =>
          text.fromJson[Json] match
            case Right(value: Json.Obj) => ZIO.succeed(value)
            case Right(value)           => ZIO.succeed(Json.Obj("result" -> value))
            case Left(_)                => ZIO.succeed(Json.Obj("result" -> Json.Str(text)))
      _ <- Console.printLine(s"[synthetic:mcp] <- $tool (${normalized.toJson.length} chars)").orDie
    yield normalized

  private def lexicalCandidates(items: Chunk[Json], query: String): Chunk[Json] =
    val stopWords = Set(
      "summarize", "classes", "class", "latest", "release", "related", "public",
      "that", "with", "from", "type", "library", "jackson", "databind"
    )
    val roots = query.toLowerCase.split("[^a-z0-9]+").iterator
      .filter(word => word.length >= 5 && !stopWords.contains(word))
      .map:
        case word if word.startsWith("polymorph") => "polymorph"
        case word if word.startsWith("validat")   => "validat"
        case word if word.startsWith("subtype")   => "subtype"
        case word                                  => word
      .toSet
    val matched = items.filter: item =>
      val text = item.toJson.toLowerCase
      roots.exists(text.contains)
    if matched.nonEmpty then matched else items

  private def lexicalPrefilter(items: Chunk[Json], query: String, limit: Int): Chunk[Json] =
    val stopWords = Set("summarize", "classes", "class", "latest", "release", "related", "public", "that", "with", "from")
    val tokens = query.toLowerCase.split("[^a-z0-9]+").iterator
      .filter(token => token.length >= 4 && !stopWords.contains(token)).toSet
    val ranked = items.zipWithIndex.map: (item, index) =>
      val text = item.toJson.toLowerCase
      val score = tokens.iterator.map(token => if text.contains(token) then 1 else 0).sum
      (item, index, score)
    val matches = ranked.filter(_._3 > 0).sortBy(entry => (-entry._3, entry._2))
    val remainder = ranked.filter(_._3 == 0).sortBy(_._2)
    Chunk.fromIterable((matches ++ remainder).take(limit).map(_._1))

  private def compact(value: Json, policy: ExecutionPolicy): Json =
    if policy.unbounded then value
    else value match
      case Json.Str(text) if text.length > 8000 => Json.Str(text.take(8000) + "…[truncated]")
      case Json.Arr(values) => Json.Arr(values.take(6).map(compact(_, policy)))
      case Json.Obj(fields) => Json.Obj(fields.map((name, child) => name -> compact(child, policy)))
      case other            => other

