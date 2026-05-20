import com.jamesward.zio_bedrock_converse.Bedrock
import com.jamesward.zio_bedrock_converse.Bedrock.ToolHandler
import zio.*
import zio.http.*
import zio.direct.*
import zio.schema.{Schema, derived}

object Main extends ZIOAppDefault:

  case class Food(name: String, region: String) derives Schema

  val program = defer:

    // BASIC INFERENCE

    Bedrock.converse("say hello").text.debug("basic text").run

    Bedrock.converse("Favorite food").as[Food].debug("structured output").run

    Bedrock.converse("tell a one-line joke").asResponse.debug("detailed response").run

    Bedrock.converse("Worst food").asResponse[Food].debug("detailed response with structured output").run

    Bedrock.converse("Write a poem about Scala").textStream
      .runForeach(Console.print(_).orDie).run


    // TOOL LOOP

    // note that inputs & outputs can be classes with Schemas to provide property descriptions
    val tools = (
      // tool with side effects
      randomLetters = ToolHandler(
        (n: Int) => Random.nextIntBounded(26).replicateZIO(n).map(_.map(i => ('a' + i).toChar).mkString).debug("\nrandom letters"),
        "generate n number of random letters"
      ),
      // pure tool
      reverse = ToolHandler.fromPure((s: String) => { println("\nreverse"); s.reverse }, "reverse a string")
    )

    Bedrock.loop("generate 8 random letters", tools).text.debug("multi-turn loop with tool call").run

    Bedrock.loop("generate 8 random letters and then come up with a food name that is similar", tools).as[Food].debug("multi-turn with structured outputs").run

    // multiple tool calls
    Bedrock.loop("display 8 random letters and its reverse", tools)
      .textStream.runForeach(Console.print(_).orDie).run


    // SINGLE TURN WITH TOOLS

    // we must handle all the possible tool calls
    Bedrock.request("generate a 16 character random string", tools).fold[Unit]:
      (
        randomLetters = s => println(s"tool result: $s"),
        reverse = s => println("should not happen")
      )
    .run

  def run = program.provide(Client.default, Bedrock.Client.live)
