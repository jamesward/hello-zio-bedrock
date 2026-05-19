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
      .runForeach(Console.print(_).orDie).debug.run


    // TOOL LOOP

    val tools = (
      // note that inputs & outputs can be classes with Schemas to provide property descriptions
      randomLetters = ToolHandler(
        (n: Int) => Random.nextPrintableChar.replicateZIO(n).map(_.mkString).debug("random letters"),
        "generate n number of random letters"
      ),
      reverse = ToolHandler.fromPure((s: String) => s.reverse, "reverse a string")
    )

    Bedrock.loop("generate 8 random letters", tools).text.debug("multi-turn loop with tool call").run

    Bedrock.loop("display 8 random letters and its reverse", tools)
      .textStream.runForeach(Console.print(_).orDie).debug.run

    "" // end of program - just here to avoid seeing () at the end of the output

  def run = program.provide(Client.default, Bedrock.Client.live)
